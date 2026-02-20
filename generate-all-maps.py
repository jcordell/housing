import duckdb
import pandas as pd
import folium
from folium.features import DivIcon
import json
import webbrowser
import os

DB_FILE = "sb79_housing.duckdb"

def analyze_and_map():
    if not os.path.exists('neighborhoods.geojson'):
        print("ERROR: 'neighborhoods.geojson' missing. Run 'download-sb79.py' first.")
        return

    # Check environment variable (defaults to true if not set)
    recalculate = os.environ.get('RECALCULATE', 'true').lower() == 'true'

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    if recalculate:
        print("Running 5-Scenario Spatial Analysis with Parcel Assembly (Caching results)...")
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE parcel_base AS
            WITH
            target_zones AS (
                SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, zone_class
                FROM zoning WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'
            ),
            projected_transit AS (
                SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM transit_stops
            ),
            projected_bus_all AS (
                SELECT CAST(route AS VARCHAR) as route, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435
                FROM bus_routes
            ),
            projected_bus_hf AS (
                SELECT geom_3435 FROM projected_bus_all
                WHERE route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')
            ),
            projected_bus_brt AS (
                SELECT geom_3435 FROM projected_bus_all WHERE route = 'J14'
            ),
            processed_parcels AS (
                SELECT pin10, SUBSTR(pin10, 1, 7) as block_id, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435
                FROM parcels WHERE geom IS NOT NULL
            ),
            parcel_zone_join AS (
                SELECT p.pin10, p.block_id, p.geom_3435, ST_Area(p.geom_3435) as area_sqft, z.zone_class
                FROM processed_parcels p
                JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)
            ),
            eligible_parcels AS (
                SELECT pin10, ANY_VALUE(block_id) as block_id, ANY_VALUE(geom_3435) as geom_3435, ANY_VALUE(area_sqft) as area_sqft, ANY_VALUE(zone_class) as zone_class
                FROM parcel_zone_join GROUP BY pin10
            ),

            -- ADVANCED LOT ASSEMBLY
            assembled_lots AS (
                SELECT
                    block_id, zone_class,
                    ST_Union_Agg(geom_3435) as assembled_geom,
                    ST_Transform(ST_Centroid(ST_Union_Agg(geom_3435)), 'EPSG:3435', 'EPSG:4326', true) as center_geom,
                    SUM(area_sqft) as assembled_area_sqft,
                    COUNT(pin10) as parcels_combined
                FROM eligible_parcels
                GROUP BY block_id, zone_class
            ),

            -- BUS ROUTE COUNTER
            parcel_bus_counts AS (
                SELECT
                    a.block_id, a.zone_class,
                    COUNT(DISTINCT b_all.route) as all_bus_count,
                    COUNT(DISTINCT CASE WHEN b_all.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b_all.route END) as hf_bus_count
                FROM assembled_lots a
                JOIN projected_bus_all b_all ON ST_Distance(a.assembled_geom, b_all.geom_3435) <= 1320
                GROUP BY a.block_id, a.zone_class
            ),

            parcel_distances AS (
                SELECT
                    a.block_id, a.center_geom, a.assembled_area_sqft as area_sqft,
                    a.parcels_combined, a.zone_class,
                    COALESCE(pbc.all_bus_count, 0) as all_bus_count,
                    COALESCE(pbc.hf_bus_count, 0) as hf_bus_count,
                    MIN(ST_Distance(a.assembled_geom, t.geom_3435)) as min_dist_train,
                    MIN(ST_Distance(a.assembled_geom, b_brt.geom_3435)) as min_dist_brt,
                    MIN(ST_Distance(a.assembled_geom, b_hf.geom_3435)) as min_dist_hf_bus
                FROM assembled_lots a
                LEFT JOIN projected_transit t ON ST_Distance(a.assembled_geom, t.geom_3435) <= 2640
                LEFT JOIN projected_bus_brt b_brt ON ST_Distance(a.assembled_geom, b_brt.geom_3435) <= 2640
                LEFT JOIN projected_bus_hf b_hf ON ST_Distance(a.assembled_geom, b_hf.geom_3435) <= 1320
                LEFT JOIN parcel_bus_counts pbc ON a.block_id = pbc.block_id AND a.zone_class = pbc.zone_class
                GROUP BY a.block_id, a.center_geom, a.assembled_area_sqft, a.parcels_combined, a.zone_class, pbc.all_bus_count, pbc.hf_bus_count
            ),

            parcel_calculations AS (
                SELECT
                    center_geom, area_sqft, zone_class, parcels_combined,

                    -- BASELINE
                    GREATEST(parcels_combined, CASE
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

                    -- SCENARIO 1: ORIGINAL UPZONING (Pritzker BUILD Act)
                    CASE
                        WHEN zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN
                            CASE
                                WHEN (area_sqft / parcels_combined) < 2500 THEN 1 * parcels_combined
                                WHEN (area_sqft / parcels_combined) < 5000 THEN 4 * parcels_combined
                                WHEN (area_sqft / parcels_combined) < 7500 THEN 6 * parcels_combined
                                ELSE 8 * parcels_combined
                            END
                        ELSE 0
                    END as pritzker_capacity,

                    -- SCENARIO 2: TRUE CALIFORNIA SB 79 (Train OR BRT OR Intersection of 2+ HF Buses)
                    CASE
                        WHEN area_sqft < 5000 THEN 0
                        WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120)
                        WHEN min_dist_train <= 2640 OR min_dist_brt <= 1320 OR hf_bus_count >= 2 THEN FLOOR((area_sqft / 43560.0) * 100)
                        WHEN min_dist_brt <= 2640 THEN FLOOR((area_sqft / 43560.0) * 80)
                        ELSE 0
                    END as cap_true_sb79,

                    -- SCENARIO 3: SB 79 TRAIN ONLY
                    CASE
                        WHEN area_sqft < 5000 THEN 0
                        WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120)
                        WHEN min_dist_train <= 2640 THEN FLOOR((area_sqft / 43560.0) * 100)
                        ELSE 0
                    END as cap_train_only,

                    -- SCENARIO 4: SB 79 TRAIN + HF BUS
                    CASE
                        WHEN area_sqft < 5000 THEN 0
                        WHEN min_dist_train <= 2640 AND min_dist_hf_bus <= 1320 THEN
                            CASE WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END
                        ELSE 0
                    END as cap_train_and_hf_bus,

                    -- SCENARIO 5: SB 79 TRAIN + (HF BUS OR 2+ BUS LINES)
                    CASE
                        WHEN area_sqft < 5000 THEN 0
                        WHEN min_dist_train <= 2640 AND (min_dist_hf_bus <= 1320 OR all_bus_count >= 2) THEN
                            CASE WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END
                        ELSE 0
                    END as cap_train_and_bus_combo

                FROM parcel_distances
            )
            SELECT
                center_geom,
                GREATEST(0, pritzker_capacity - current_capacity) as new_pritzker,

                GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) as add_true_sb79,
                GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_true_sb79) - current_capacity) as tot_true_sb79,

                GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) as add_train_only,
                GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_only) - current_capacity) as tot_train_only,

                GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) as add_train_and_hf_bus,
                GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_hf_bus) - current_capacity) as tot_train_and_hf_bus,

                GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) as add_train_and_bus_combo,
                GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_bus_combo) - current_capacity) as tot_train_and_bus_combo

            FROM parcel_calculations;
        """)

        print("Extracting Neighborhood Aggregates and writing to permanent cache...")

        con.execute("""
            CREATE OR REPLACE TABLE neighborhood_results AS
            SELECT
                n.community as neighborhood_name,
                SUM(pb.new_pritzker) as new_pritzker,

                SUM(pb.add_true_sb79) as add_true_sb79,
                SUM(pb.tot_true_sb79) as tot_true_sb79,

                SUM(pb.add_train_only) as add_train_only,
                SUM(pb.tot_train_only) as tot_train_only,

                SUM(pb.add_train_and_hf_bus) as add_train_and_hf_bus,
                SUM(pb.tot_train_and_hf_bus) as tot_train_and_hf_bus,

                SUM(pb.add_train_and_bus_combo) as add_train_and_bus_combo,
                SUM(pb.tot_train_and_bus_combo) as tot_train_and_bus_combo,

                ST_Y(ST_Centroid(n.geom)) as label_lat,
                ST_X(ST_Centroid(n.geom)) as label_lon
            FROM parcel_base pb
            JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pb.center_geom, n.geom)
            GROUP BY n.community, n.geom
            HAVING SUM(pb.tot_true_sb79) > 0 OR SUM(pb.tot_train_and_bus_combo) > 0
        """)

        df_neighborhoods = con.execute("SELECT * FROM neighborhood_results ORDER BY tot_train_and_bus_combo DESC").df()

    else:
        print("RECALCULATE is false. Skipping heavy math and loading cached dataset...")
        try:
            df_neighborhoods = con.execute("SELECT * FROM neighborhood_results ORDER BY tot_train_and_bus_combo DESC").df()
        except Exception as e:
            print("❌ ERROR: Cached table 'neighborhood_results' not found. Please run: RECALCULATE=true python3 generate-all-maps.py")
            con.close()
            return

    con.close()

    if df_neighborhoods.empty:
        print("No data found.")
        return

    # ---------------------------------------------------------
    # TERMINAL OUTPUT
    # ---------------------------------------------------------
    print("\n" + "="*80)
    print("HOUSING POLICY IMPACT ANALYSIS: SB 79 BUS VS TRAIN MODELING")
    print("="*80)
    print(f"1. Original Pritzker Upzoning (Net New):         {df_neighborhoods['new_pritzker'].sum():,.0f}")
    print("-" * 80)
    print(f"2. TRUE CA SB 79 (Trains + BRT/Bus Intersections): {df_neighborhoods['tot_true_sb79'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_true_sb79'].sum():,.0f}")
    print(f"3. SB 79: Trains Only:                           {df_neighborhoods['tot_train_only'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_only'].sum():,.0f}")
    print(f"4. SB 79 TRAIN + HIGH FREQ BUS:                  {df_neighborhoods['tot_train_and_hf_bus'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}")
    print(f"5. SB 79 TRAIN + (HIGH FREQ BUS OR 2+ BUS LINES): {df_neighborhoods['tot_train_and_bus_combo'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}")
    print("="*80 + "\n")

    # ---------------------------------------------------------
    # MAPPING
    # ---------------------------------------------------------
    with open('neighborhoods.geojson', 'r') as f:
        geo_data = json.load(f)

    df_neighborhoods['neighborhood_name'] = df_neighborhoods['neighborhood_name'].str.upper()
    unit_lookup = df_neighborhoods.set_index('neighborhood_name').to_dict('index')

    for feature in geo_data['features']:
        name = feature['properties']['community'].upper()
        stats = unit_lookup.get(name, {})

        # Pritzker
        feature['properties']['m1_val'] = f"{stats.get('new_pritzker', 0):,.0f}"

        # Map 2 additions
        feature['properties']['m2_val'] = f"{stats.get('tot_true_sb79', 0):,.0f}"
        feature['properties']['m2_diff'] = f"+{stats.get('add_true_sb79', 0):,.0f}"

        # Map 3 additions
        feature['properties']['m3_val'] = f"{stats.get('tot_train_only', 0):,.0f}"
        feature['properties']['m3_diff'] = f"+{stats.get('add_train_only', 0):,.0f}"

        # Map 4 additions
        feature['properties']['m4_val'] = f"{stats.get('tot_train_and_hf_bus', 0):,.0f}"
        feature['properties']['m4_diff'] = f"+{stats.get('add_train_and_hf_bus', 0):,.0f}"

        # Map 5 additions
        feature['properties']['m5_val'] = f"{stats.get('tot_train_and_bus_combo', 0):,.0f}"
        feature['properties']['m5_diff'] = f"+{stats.get('add_train_and_bus_combo', 0):,.0f}"

    def add_labels(folium_map, df, col_name):
        for i, row in df.iterrows():
            units = row[col_name]
            if units >= 1000: label_text = f"{int(round(units/1000))}k"
            elif units > 0: label_text = "<1k"
            else: continue
            label_html = f'''<div style="font-family: sans-serif; font-size: 8pt; color: white; text-shadow: 1px 1px 2px black; text-align: center; white-space: nowrap; transform: translate(-50%, -50%); pointer-events: none;">{label_text}</div>'''
            folium.map.Marker([row['label_lat'], row['label_lon']], icon=DivIcon(icon_size=(50,20), icon_anchor=(0,0), html=label_html)).add_to(folium_map)

    def create_map(title, data_col, tooltip_fields, tooltip_aliases, output_file, tot_val, add_val):
        print(f"Generating: {title}...")
        m = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")

        # Create choropleth but DO NOT add it to the map yet
        choro = folium.Choropleth(
            geo_data=geo_data, name=title, data=df_neighborhoods,
            columns=['neighborhood_name', data_col], key_on='feature.properties.community',
            fill_color='Greens', fill_opacity=0.7, line_opacity=0.2, line_color='white'
        )

        # HACK: Iterate through the choropleth's children and permanently delete the color legend
        for key in list(choro._children.keys()):
            if key.startswith('color_map'):
                del(choro._children[key])

        # Now add the clean choropleth to the map
        choro.add_to(m)

        add_labels(m, df_neighborhoods, data_col)

        folium.GeoJson(
            geo_data, style_function=lambda x: {'fillColor': '#ffffff', 'color':'transparent', 'fillOpacity': 0.0},
            highlight_function=lambda x: {'fillColor': '#ffffff', 'color':'white', 'fillOpacity': 0.2, 'weight': 2},
            tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=tooltip_aliases, style="background-color: black; color: white;")
        ).add_to(m)

        # Inject Custom HTML Legend into the top right
        legend_html = f'''
        <div style="
            position: fixed;
            top: 20px;
            right: 20px;
            width: 290px;
            background-color: rgba(30, 30, 30, 0.9);
            color: #ffffff;
            z-index: 9999;
            border: 1px solid #777;
            padding: 15px;
            border-radius: 8px;
            font-family: sans-serif;
            pointer-events: auto;
            box-shadow: 2px 2px 8px rgba(0,0,0,0.5);
        ">
            <h4 style="margin-top: 0; margin-bottom: 10px; font-size: 16px; border-bottom: 1px solid #555; padding-bottom: 5px;">
                {title}
            </h4>
            <p style="margin: 0; font-size: 14px; line-height: 1.6;">
                <b>Total Net New Units:</b> {tot_val:,.0f}<br>
                <span style="color: #4CAF50;"><b>Additional vs Pritzker:</b> {add_val}</span>
            </p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))

        m.save(output_file)

    # 1. Pritzker
    create_map("Map 1: Pritzker Upzoning", 'new_pritzker',
               ['community', 'm1_val'], ['Neighborhood:', 'Pritzker Units:'],
               "map_1_pritzker.html", df_neighborhoods['new_pritzker'].sum(), "N/A (Baseline)")

    # 2. True CA SB 79
    create_map("Map 2: TRUE CA SB 79 (Train + BRT/Intersections)", 'tot_true_sb79',
               ['community', 'm2_val', 'm2_diff'], ['Neighborhood:', 'Total SB 79 Units:', 'Additional vs Pritzker:'],
               "map_2_sb79_true.html", df_neighborhoods['tot_true_sb79'].sum(), f"+{df_neighborhoods['add_true_sb79'].sum():,.0f}")

    # 3. Train Only
    create_map("Map 3: SB 79 Train Only", 'tot_train_only',
               ['community', 'm3_val', 'm3_diff'], ['Neighborhood:', 'Total Units:', 'Additional vs Pritzker:'],
               "map_3_sb79_train.html", df_neighborhoods['tot_train_only'].sum(), f"+{df_neighborhoods['add_train_only'].sum():,.0f}")

    # 4. Train + HF Bus
    create_map("Map 4: SB 79 Train + HF Bus", 'tot_train_and_hf_bus',
               ['community', 'm4_val', 'm4_diff'], ['Neighborhood:', 'Total Units:', 'Additional vs Pritzker:'],
               "map_4_sb79_train_hf.html", df_neighborhoods['tot_train_and_hf_bus'].sum(), f"+{df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}")

    # 5. Train + Bus Options
    create_map("Map 5: SB 79 Train + (HF Bus OR 2 Bus Lines)", 'tot_train_and_bus_combo',
               ['community', 'm5_val', 'm5_diff'], ['Neighborhood:', 'Total Units:', 'Additional vs Pritzker:'],
               "map_5_sb79_train_hf_any2.html", df_neighborhoods['tot_train_and_bus_combo'].sum(), f"+{df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}")

    print("✅ All 5 maps generated successfully!")

    for map_file in ["map_1_pritzker.html", "map_2_sb79_true.html", "map_3_sb79_train.html", "map_4_sb79_train_hf.html", "map_5_sb79_train_hf_any2.html"]:
        try:
            webbrowser.open('file://' + os.path.realpath(map_file))
        except:
            pass

if __name__ == "__main__":
    analyze_and_map()
