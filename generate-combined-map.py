import duckdb
import pandas as pd
import folium
from folium.features import DivIcon
import json
import webbrowser
import os

DB_FILE = "sb79_housing.duckdb"
OUTPUT_MAP = "chicago_sb79_map.html"

def analyze_and_map():
    if not os.path.exists('neighborhoods.geojson'):
        print("ERROR: 'neighborhoods.geojson' missing. Run 'download-sb79.py' first.")
        return

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    print("Running spatial analysis for Pritzker's BUILD Act + SB 79 equivalents...")

    query = """
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
    processed_parcels AS (
        SELECT
            pin10,
            ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,
            ST_Centroid(geom) as center_geom
        FROM parcels
        WHERE geom IS NOT NULL
    ),
    parcel_zone_join AS (
        SELECT
            p.pin10,
            p.geom_3435,
            p.center_geom,
            ST_Area(p.geom_3435) as area_sqft,
            z.zone_class
        FROM processed_parcels p
        JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)
    ),
    eligible_parcels AS (
        SELECT
            pin10,
            ANY_VALUE(geom_3435) as geom_3435,
            ANY_VALUE(center_geom) as center_geom,
            ANY_VALUE(area_sqft) as area_sqft,
            ANY_VALUE(zone_class) as zone_class
        FROM parcel_zone_join
        GROUP BY pin10
    ),
    parcel_distances AS (
        SELECT
            p.pin10,
            p.center_geom,
            p.area_sqft,
            p.zone_class,
            MIN(ST_Distance(p.geom_3435, t.geom_3435)) as min_dist_ft
        FROM eligible_parcels p
        LEFT JOIN projected_transit t ON ST_Distance(p.geom_3435, t.geom_3435) <= 2640
        GROUP BY p.pin10, p.center_geom, p.area_sqft, p.zone_class
    ),
    parcel_calculations AS (
        SELECT
            center_geom,
            area_sqft,
            zone_class,

            -- 1. BASELINE: Current Allowed Units
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

            -- 2. ORIGINAL UPZONING (Pritzker BUILD Act):
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

            -- 3. SB79 CAPACITY:
            CASE
                WHEN min_dist_ft IS NOT NULL AND min_dist_ft <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120)
                WHEN min_dist_ft IS NOT NULL AND min_dist_ft <= 2640 THEN FLOOR((area_sqft / 43560.0) * 100)
                ELSE 0
            END as sb79_capacity
        FROM parcel_distances
    ),
    parcel_net_new AS (
        SELECT
            center_geom,
            -- New units from the original bill alone
            GREATEST(0, pritzker_capacity - current_capacity) as pritzker_new,
            -- Additional units SB 79 provides ON TOP of the original bill
            GREATEST(0, sb79_capacity - GREATEST(current_capacity, pritzker_capacity)) as sb79_additional,
            -- Total combined
            GREATEST(0, GREATEST(current_capacity, pritzker_capacity, sb79_capacity) - current_capacity) as total_combined_new
        FROM parcel_calculations
    ),
    neighborhood_stats AS (
        SELECT
            n.community as neighborhood_name,
            SUM(pnn.pritzker_new) as total_pritzker_new,
            SUM(pnn.sb79_additional) as total_sb79_additional,
            SUM(pnn.total_combined_new) as total_combined_new,
            ST_Y(ST_Centroid(n.geom)) as label_lat,
            ST_X(ST_Centroid(n.geom)) as label_lon
        FROM parcel_net_new pnn
        JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pnn.center_geom, n.geom)
        GROUP BY n.community, n.geom
    )
    SELECT * FROM neighborhood_stats WHERE total_combined_new > 0 ORDER BY total_combined_new DESC;
    """

    try:
        df = con.execute(query).df()
    except Exception as e:
        print(f"Query Error: {e}")
        con.close()
        return

    con.close()

    if df.empty:
        print("No data found.")
        return

    # ---------------------------------------------------------
    # EXACT TERMINAL OUTPUT REQUESTED
    # ---------------------------------------------------------
    pritzker_total = df['total_pritzker_new'].sum()
    sb79_add_total = df['total_sb79_additional'].sum()

    print(f"Total new units allowed by original upzoning: {pritzker_total:,.0f}")
    print(f"Additional units allowed by sb79 equivilant: {sb79_add_total:,.0f}")

    # ---------------------------------------------------------
    # MAPPING
    # ---------------------------------------------------------
    print("Generating Combined Map...")

    with open('neighborhoods.geojson', 'r') as f:
        geo_data = json.load(f)

    df['neighborhood_name'] = df['neighborhood_name'].str.upper()

    m = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")

    folium.Choropleth(
        geo_data=geo_data,
        name="Combined Housing Potential",
        data=df,
        columns=['neighborhood_name', 'total_combined_new'],
        key_on='feature.properties.community',
        fill_color='YlGnBu',
        fill_opacity=0.7,
        line_opacity=0.2,
        line_color='white',
        legend_name='Total Combined New Units'
    ).add_to(m)

    for i, row in df.iterrows():
        units = row['total_combined_new']
        lat = row['label_lat']
        lon = row['label_lon']

        if units >= 1000:
            label_text = f"{int(round(units/1000))}k"
        elif units > 0:
            label_text = "<1k"
        else:
            continue

        label_html = f'''
            <div style="
                font-family: sans-serif;
                font-size: 8pt;
                color: white;
                text-shadow: 1px 1px 2px black;
                text-align: center;
                white-space: nowrap;
                transform: translate(-50%, -50%);
                pointer-events: none;
            ">
            {label_text}
            </div>
        '''

        folium.map.Marker(
            [lat, lon],
            icon=DivIcon(
                icon_size=(50,20),
                icon_anchor=(0,0),
                html=label_html
            )
        ).add_to(m)

    unit_lookup = df.set_index('neighborhood_name').to_dict('index')

    for feature in geo_data['features']:
        name = feature['properties']['community'].upper()
        stats = unit_lookup.get(name, {})
        feature['properties']['pritzker_display'] = f"{stats.get('total_pritzker_new', 0):,.0f}"
        feature['properties']['sb79_additional_display'] = f"{stats.get('total_sb79_additional', 0):,.0f}"
        feature['properties']['total_display'] = f"{stats.get('total_combined_new', 0):,.0f}"

    folium.GeoJson(
        geo_data,
        style_function=lambda x: {'fillColor': '#ffffff', 'color':'transparent', 'fillOpacity': 0.0},
        highlight_function=lambda x: {'fillColor': '#ffffff', 'color':'white', 'fillOpacity': 0.2, 'weight': 2},
        tooltip=folium.GeoJsonTooltip(
            fields=['community', 'pritzker_display', 'sb79_additional_display', 'total_display'],
            aliases=['Neighborhood:', 'Original Upzoning:', 'SB79 Additional:', 'Total Combined:'],
            style="background-color: black; color: white; border-radius: 5px; font-family: sans-serif;"
        )
    ).add_to(m)

    m.save(OUTPUT_MAP)
    print(f"Map saved to {OUTPUT_MAP}")

    try:
        webbrowser.open('file://' + os.path.realpath(OUTPUT_MAP))
    except:
        pass

if __name__ == "__main__":
    analyze_and_map()
