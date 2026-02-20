import duckdb
import pandas as pd
import folium
from folium.plugins import HeatMap
from folium.features import DivIcon
import json
import webbrowser
import os

DB_FILE = "sb79_housing.duckdb"

def analyze_and_map():
    if not os.path.exists('neighborhoods.geojson'):
        print("ERROR: 'neighborhoods.geojson' missing. Run 'download-sb79.py' first.")
        return

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    print("Running heavy spatial analysis (Caching results)...")

    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE parcel_base AS
        WITH
        target_zones AS (
            SELECT
                ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,
                zone_class
            FROM zoning
            WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'
        ),
        projected_transit AS (
            SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435
            FROM transit_stops
        ),
        projected_bus AS (
            SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435
            FROM bus_routes
            -- Filtering specifically for Chicago's 10-Minute "Frequent Network" routes
            WHERE CAST(route AS VARCHAR) IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')
        ),
        processed_parcels AS (
            SELECT
                pin10,
                ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,
                ST_Centroid(geom) as center_geom
            FROM parcels
            WHERE geom IS NOT NULL
        ),
        parcel_zone_join AS (
            SELECT p.pin10, p.geom_3435, p.center_geom, ST_Area(p.geom_3435) as area_sqft, z.zone_class
            FROM processed_parcels p
            JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)
        ),
        eligible_parcels AS (
            SELECT pin10, ANY_VALUE(geom_3435) as geom_3435, ANY_VALUE(center_geom) as center_geom, ANY_VALUE(area_sqft) as area_sqft, ANY_VALUE(zone_class) as zone_class
            FROM parcel_zone_join
            GROUP BY pin10
        ),
        parcel_distances AS (
            SELECT
                p.pin10, p.center_geom, p.area_sqft, p.zone_class,
                MIN(ST_Distance(p.geom_3435, t.geom_3435)) as min_dist_train,
                MIN(ST_Distance(p.geom_3435, b.geom_3435)) as min_dist_bus
            FROM eligible_parcels p
            LEFT JOIN projected_transit t ON ST_Distance(p.geom_3435, t.geom_3435) <= 2640
            LEFT JOIN projected_bus b ON ST_Distance(p.geom_3435, b.geom_3435) <= 1320
            GROUP BY p.pin10, p.center_geom, p.area_sqft, p.zone_class
        ),
        parcel_calculations AS (
            SELECT
                center_geom, area_sqft, zone_class,

                -- 1. BASELINE
                GREATEST(1, CASE
                    WHEN zone_class LIKE 'RS-1%' OR zone_class LIKE 'RS-2%' THEN FLOOR(area_sqft / 5000)
                    WHEN zone_class LIKE 'RS-3%' THEN FLOOR(area_sqft / 2500)
                    WHEN zone_class LIKE 'RT-3.5%' THEN FLOOR(area_sqft / 1250)
                    WHEN zone_class LIKE 'RT-4%' THEN FLOOR(area_sqft / 1000)
                    WHEN zone_class LIKE 'RM-4.5%' OR zone_class LIKE 'RM-5%' THEN FLOOR(area_sqft / 400)
                    WHEN zone_class LIKE 'RM-6%' OR zone_class LIKE 'RM-6.5%' THEN FLOOR(area_sqft / 200)
                    WHEN zone_class LIKE '%-1' THEN FLOOR(area_sqft / 1000)
                    WHEN zone_class LIKE '%-2' OR zone_class LIKE '%-3' THEN FLOOR(area_sqft / 400)
                    WHEN zone_class LIKE '%-5' OR zone_class LIKE '%-6' THEN FLOOR(area_sqft / 200)
                    ELSE FLOOR(area_sqft / 1000)
                END) as current_capacity,

                -- 2. ORIGINAL UPZONING (Pritzker BUILD Act)
                CASE
                    WHEN zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN
                        CASE
                            WHEN area_sqft < 2500 THEN 1
                            WHEN area_sqft >= 2500 AND area_sqft < 5000 THEN 4
                            WHEN area_sqft >= 5000 AND area_sqft < 7500 THEN 6
                            WHEN area_sqft >= 7500 THEN 8
                            ELSE 1
                        END
                    ELSE 0
                END as pritzker_capacity,

                -- 3. SB79 FULL CAPACITY (Train Only)
                CASE
                    WHEN min_dist_train IS NOT NULL AND min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120)
                    WHEN min_dist_train IS NOT NULL AND min_dist_train <= 2640 THEN FLOOR((area_sqft / 43560.0) * 100)
                    ELSE 0
                END as sb79_full_capacity,

                -- 4. SB79 RESTRICTED CAPACITY (Train AND 10-Min Bus)
                CASE
                    WHEN min_dist_train IS NOT NULL AND min_dist_bus IS NOT NULL AND min_dist_bus <= 1320 THEN
                        CASE
                            WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120)
                            WHEN min_dist_train <= 2640 THEN FLOOR((area_sqft / 43560.0) * 100)
                            ELSE 0
                        END
                    ELSE 0
                END as sb79_restricted_capacity

            FROM parcel_distances
        )
        SELECT
            center_geom,
            GREATEST(0, pritzker_capacity - current_capacity) as pritzker_new,

            -- Full SB79 calculations
            GREATEST(0, sb79_full_capacity - GREATEST(current_capacity, pritzker_capacity)) as sb79_full_additional,
            GREATEST(0, GREATEST(current_capacity, pritzker_capacity, sb79_full_capacity) - current_capacity) as total_combined_full,

            -- Restricted SB79 calculations
            GREATEST(0, sb79_restricted_capacity - GREATEST(current_capacity, pritzker_capacity)) as sb79_restricted_additional,
            GREATEST(0, GREATEST(current_capacity, pritzker_capacity, sb79_restricted_capacity) - current_capacity) as total_combined_restricted

        FROM parcel_calculations;
    """)

    # --- QUERY 1: Aggregated Neighborhood Stats ---
    print("Extracting Neighborhood Aggregates...")
    df_neighborhoods = con.execute("""
        SELECT
            n.community as neighborhood_name,
            SUM(pb.pritzker_new) as total_pritzker_new,
            SUM(pb.sb79_full_additional) as total_sb79_full_additional,
            SUM(pb.sb79_restricted_additional) as total_sb79_restricted_additional,
            SUM(pb.total_combined_full) as total_combined_full,
            SUM(pb.total_combined_restricted) as total_combined_restricted,
            ST_Y(ST_Centroid(n.geom)) as label_lat,
            ST_X(ST_Centroid(n.geom)) as label_lon
        FROM parcel_base pb
        JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pb.center_geom, n.geom)
        GROUP BY n.community, n.geom
        HAVING total_combined_full > 0
        ORDER BY total_combined_full DESC;
    """).df()

    # --- QUERY 2: Optimized Point Data for Heatmap (Restricted Units Only) ---
    print("Extracting and Optimizing Data for Heatmap...")
    df_heatmap = con.execute("""
        SELECT
            ROUND(ST_Y(center_geom), 3) as lat,
            ROUND(ST_X(center_geom), 3) as lon,
            SUM(sb79_restricted_additional) as weight
        FROM parcel_base
        WHERE sb79_restricted_additional > 0
        GROUP BY lat, lon
    """).df()

    con.close()

    if df_neighborhoods.empty:
        print("No data found.")
        return

    # ---------------------------------------------------------
    # TERMINAL OUTPUT
    # ---------------------------------------------------------
    pritzker_total = df_neighborhoods['total_pritzker_new'].sum()
    sb79_full_add_total = df_neighborhoods['total_sb79_full_additional'].sum()
    sb79_restrict_add_total = df_neighborhoods['total_sb79_restricted_additional'].sum()
    combined_full_total = df_neighborhoods['total_combined_full'].sum()
    combined_restrict_total = df_neighborhoods['total_combined_restricted'].sum()

    print("\n" + "="*70)
    print("HOUSING POLICY IMPACT ANALYSIS (CHICAGO)")
    print("="*70)
    print(f"Total new units allowed by original upzoning:      {pritzker_total:,.0f}")
    print(f"Additional units allowed by full sb79 equivalent:  {sb79_full_add_total:,.0f}")
    print(f"Additional units allowed by restricted train+bus:  {sb79_restrict_add_total:,.0f}")
    print("-" * 70)
    print(f"TOTAL COMBINED (Pritzker + Full SB79):             {combined_full_total:,.0f}")
    print(f"TOTAL COMBINED (Pritzker + Restricted Train+Bus):  {combined_restrict_total:,.0f}")
    print("="*70 + "\n")

    # Load GeoJSON once
    with open('neighborhoods.geojson', 'r') as f:
        geo_data = json.load(f)

    df_neighborhoods['neighborhood_name'] = df_neighborhoods['neighborhood_name'].str.upper()
    unit_lookup = df_neighborhoods.set_index('neighborhood_name').to_dict('index')

    for feature in geo_data['features']:
        name = feature['properties']['community'].upper()
        stats = unit_lookup.get(name, {})
        feature['properties']['pritzker_display'] = f"{stats.get('total_pritzker_new', 0):,.0f}"
        feature['properties']['sb79_full_display'] = f"{stats.get('total_sb79_full_additional', 0):,.0f}"
        feature['properties']['sb79_rest_display'] = f"{stats.get('total_sb79_restricted_additional', 0):,.0f}"
        feature['properties']['total_full_display'] = f"{stats.get('total_combined_full', 0):,.0f}"
        feature['properties']['total_rest_display'] = f"{stats.get('total_combined_restricted', 0):,.0f}"

    def add_labels(folium_map, df, col_name):
        for i, row in df.iterrows():
            units = row[col_name]
            if units >= 1000:
                label_text = f"{int(round(units/1000))}k"
            elif units > 0:
                label_text = "<1k"
            else:
                continue

            label_html = f'''
                <div style="font-family: sans-serif; font-size: 8pt; color: white;
                    text-shadow: 1px 1px 2px black; text-align: center; white-space: nowrap;
                    transform: translate(-50%, -50%); pointer-events: none;">
                {label_text}
                </div>
            '''
            folium.map.Marker(
                [row['label_lat'], row['label_lon']],
                icon=DivIcon(icon_size=(50,20), icon_anchor=(0,0), html=label_html)
            ).add_to(folium_map)

    # MAP 1: PRITZKER ONLY
    m1 = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")
    folium.Choropleth(
        geo_data=geo_data, name="Original Upzoning", data=df_neighborhoods,
        columns=['neighborhood_name', 'total_pritzker_new'], key_on='feature.properties.community',
        fill_color='Greens', fill_opacity=0.7, line_opacity=0.2, line_color='white'
    ).add_to(m1)
    add_labels(m1, df_neighborhoods, 'total_pritzker_new')
    folium.GeoJson(
        geo_data, style_function=lambda x: {'fillColor': '#ffffff', 'color':'transparent', 'fillOpacity': 0.0},
        highlight_function=lambda x: {'fillColor': '#ffffff', 'color':'white', 'fillOpacity': 0.2, 'weight': 2},
        tooltip=folium.GeoJsonTooltip(fields=['community', 'pritzker_display'], aliases=['Neighborhood:', 'Pritzker Units:'], style="background-color: black; color: white;")
    ).add_to(m1)
    m1.save("chicago_map_1_pritzker.html")

    # MAP 2: COMBINED FULL (Train Only SB79)
    m2 = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")
    folium.Choropleth(
        geo_data=geo_data, name="Combined Full Upzoning", data=df_neighborhoods,
        columns=['neighborhood_name', 'total_combined_full'], key_on='feature.properties.community',
        fill_color='YlGnBu', fill_opacity=0.7, line_opacity=0.2, line_color='white'
    ).add_to(m2)
    add_labels(m2, df_neighborhoods, 'total_combined_full')
    folium.GeoJson(
        geo_data, style_function=lambda x: {'fillColor': '#ffffff', 'color':'transparent', 'fillOpacity': 0.0},
        highlight_function=lambda x: {'fillColor': '#ffffff', 'color':'white', 'fillOpacity': 0.2, 'weight': 2},
        tooltip=folium.GeoJsonTooltip(fields=['community', 'pritzker_display', 'sb79_full_display', 'total_full_display'],
                                      aliases=['Neighborhood:', 'Pritzker:', 'SB79 (Train):', 'Total Combined:'],
                                      style="background-color: black; color: white;")
    ).add_to(m2)
    m2.save("chicago_map_2_combined_full.html")

    # MAP 3: COMBINED RESTRICTED (Train + Bus)
    m3 = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")
    folium.Choropleth(
        geo_data=geo_data, name="Combined Restricted Upzoning", data=df_neighborhoods,
        columns=['neighborhood_name', 'total_combined_restricted'], key_on='feature.properties.community',
        fill_color='OrRd', fill_opacity=0.7, line_opacity=0.2, line_color='white'
    ).add_to(m3)
    add_labels(m3, df_neighborhoods, 'total_combined_restricted')
    folium.GeoJson(
        geo_data, style_function=lambda x: {'fillColor': '#ffffff', 'color':'transparent', 'fillOpacity': 0.0},
        highlight_function=lambda x: {'fillColor': '#ffffff', 'color':'white', 'fillOpacity': 0.2, 'weight': 2},
        tooltip=folium.GeoJsonTooltip(fields=['community', 'pritzker_display', 'sb79_rest_display', 'total_rest_display'],
                                      aliases=['Neighborhood:', 'Pritzker:', 'SB79 (Train+Bus):', 'Total Combined:'],
                                      style="background-color: black; color: white;")
    ).add_to(m3)
    m3.save("chicago_map_3_combined_restricted.html")

    # MAP 4: SB 79 RESTRICTED HEATMAP
    m4 = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")
    heat_data = df_heatmap[['lat', 'lon', 'weight']].values.tolist()
    HeatMap(
        data=heat_data,
        name="SB 79 Restricted Density Heatmap",
        radius=15, blur=15, max_zoom=1,
        gradient={0.2: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'yellow', 1.0: 'red'}
    ).add_to(m4)
    m4.save("chicago_map_4_sb79_restricted_heatmap.html")

    print("✅ All 4 maps generated successfully!")

    for map_file in ["chicago_map_1_pritzker.html", "chicago_map_2_combined_full.html", "chicago_map_3_combined_restricted.html", "chicago_map_4_sb79_restricted_heatmap.html"]:
        try:
            webbrowser.open('file://' + os.path.realpath(map_file))
        except:
            pass

if __name__ == "__main__":
    analyze_and_map()
