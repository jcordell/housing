import duckdb

DB_FILE = "sb79_housing.duckdb"

def run_parcel_calculations():
    print("Running 5-Scenario Spatial Analysis with UCLA Feasibility Filters (Caching results)...")
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE parcel_base AS
        WITH
        target_zones AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, zone_class FROM zoning WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'),
        projected_transit AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM transit_stops),
        projected_bus_all AS (SELECT CAST(route AS VARCHAR) as route, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM bus_routes),
        projected_bus_hf AS (SELECT geom_3435 FROM projected_bus_all WHERE route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')),
        projected_bus_brt AS (SELECT geom_3435 FROM projected_bus_all WHERE route = 'J14'),
        processed_parcels AS (SELECT pin10, SUBSTR(pin10, 1, 7) as block_id, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM parcels WHERE geom IS NOT NULL),

        -- NEW: Map Cook County Property Class codes to Estimated Units and Property Types
        assessor_data AS (
            SELECT
                SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
                CAST("class" AS VARCHAR) as property_class,
                CASE
                    WHEN CAST("class" AS VARCHAR) IN ('202','203','204','205','206','207','208','209','210', '234', '278') THEN 1.0
                    WHEN CAST("class" AS VARCHAR) = '211' THEN 2.0
                    WHEN CAST("class" AS VARCHAR) = '212' THEN 3.0
                    WHEN CAST("class" AS VARCHAR) = '213' THEN 5.0  -- Avg for 4 to 6 units
                    WHEN CAST("class" AS VARCHAR) = '214' THEN 10.0 -- Avg for 7+ units
                    ELSE 1.0
                END as estimated_existing_units
            FROM assessor_universe
        ),

        parcel_zone_join AS (SELECT p.pin10, p.block_id, p.geom_3435, ST_Area(p.geom_3435) as area_sqft, z.zone_class FROM processed_parcels p JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)),

        eligible_parcels AS (
            SELECT pz.pin10, ANY_VALUE(pz.block_id) as block_id, ANY_VALUE(pz.geom_3435) as geom_3435, ANY_VALUE(pz.area_sqft) as area_sqft, ANY_VALUE(pz.zone_class) as zone_class,
            ANY_VALUE(a.property_class) as property_class, SUM(a.estimated_existing_units) as existing_units
            FROM parcel_zone_join pz
            LEFT JOIN assessor_data a ON pz.pin10 = a.pin10
            GROUP BY pz.pin10
        ),

        assembled_lots AS (
            SELECT block_id, zone_class, ST_Union_Agg(geom_3435) as assembled_geom, ST_Transform(ST_Centroid(ST_Union_Agg(geom_3435)), 'EPSG:3435', 'EPSG:4326', true) as center_geom,
            SUM(area_sqft) as assembled_area_sqft, COUNT(pin10) as parcels_combined,
            SUM(existing_units) as tot_existing_units, ANY_VALUE(property_class) as primary_prop_class
            FROM eligible_parcels GROUP BY block_id, zone_class
        ),

        parcel_bus_counts AS (
            SELECT a.block_id, a.zone_class, COUNT(DISTINCT b_all.route) as all_bus_count, COUNT(DISTINCT CASE WHEN b_all.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b_all.route END) as hf_bus_count
            FROM assembled_lots a JOIN projected_bus_all b_all ON ST_Distance(a.assembled_geom, b_all.geom_3435) <= 1320 GROUP BY a.block_id, a.zone_class
        ),

        parcel_distances AS (
            SELECT a.block_id, a.center_geom, a.assembled_area_sqft as area_sqft, a.parcels_combined, a.zone_class, a.tot_existing_units, a.primary_prop_class,
            COALESCE(pbc.all_bus_count, 0) as all_bus_count, COALESCE(pbc.hf_bus_count, 0) as hf_bus_count, MIN(ST_Distance(a.assembled_geom, t.geom_3435)) as min_dist_train, MIN(ST_Distance(a.assembled_geom, b_brt.geom_3435)) as min_dist_brt, MIN(ST_Distance(a.assembled_geom, b_hf.geom_3435)) as min_dist_hf_bus
            FROM assembled_lots a LEFT JOIN projected_transit t ON ST_Distance(a.assembled_geom, t.geom_3435) <= 2640 LEFT JOIN projected_bus_brt b_brt ON ST_Distance(a.assembled_geom, b_brt.geom_3435) <= 2640 LEFT JOIN projected_bus_hf b_hf ON ST_Distance(a.assembled_geom, b_hf.geom_3435) <= 1320 LEFT JOIN parcel_bus_counts pbc ON a.block_id = pbc.block_id AND a.zone_class = pbc.zone_class
            GROUP BY a.block_id, a.center_geom, a.assembled_area_sqft, a.parcels_combined, a.zone_class, a.tot_existing_units, a.primary_prop_class, pbc.all_bus_count, pbc.hf_bus_count
        ),

        parcel_calculations AS (
            SELECT center_geom, area_sqft, zone_class, parcels_combined, tot_existing_units, primary_prop_class,
                GREATEST(parcels_combined, CASE WHEN zone_class LIKE 'RS-1%' OR zone_class LIKE 'RS-2%' THEN FLOOR(area_sqft / 5000) WHEN zone_class LIKE 'RS-3%' THEN FLOOR(area_sqft / 2500) WHEN zone_class LIKE 'RT-3.5%' THEN FLOOR(area_sqft / 1250) WHEN zone_class LIKE 'RT-4%' THEN FLOOR(area_sqft / 1000) WHEN zone_class LIKE 'RM-4.5%' OR zone_class LIKE 'RM-5%' THEN FLOOR(area_sqft / 400) WHEN zone_class LIKE 'RM-6%' OR zone_class LIKE 'RM-6.5%' THEN FLOOR(area_sqft / 200) WHEN zone_class LIKE '%-1' THEN FLOOR(area_sqft / 1000) WHEN zone_class LIKE '%-2' OR zone_class LIKE '%-3' THEN FLOOR(area_sqft / 400) WHEN zone_class LIKE '%-5' OR zone_class LIKE '%-6' THEN FLOOR(area_sqft / 200) ELSE FLOOR(area_sqft / 1000) END) as current_capacity,
                CASE WHEN zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN CASE WHEN (area_sqft / parcels_combined) < 2500 THEN 1 * parcels_combined WHEN (area_sqft / parcels_combined) < 5000 THEN 4 * parcels_combined WHEN (area_sqft / parcels_combined) < 7500 THEN 6 * parcels_combined ELSE 8 * parcels_combined END ELSE 0 END as pritzker_capacity,
                CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) WHEN min_dist_train <= 2640 OR min_dist_brt <= 1320 OR hf_bus_count >= 2 THEN FLOOR((area_sqft / 43560.0) * 100) WHEN min_dist_brt <= 2640 THEN FLOOR((area_sqft / 43560.0) * 80) ELSE 0 END as cap_true_sb79,
                CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) WHEN min_dist_train <= 2640 THEN FLOOR((area_sqft / 43560.0) * 100) ELSE 0 END as cap_train_only,
                CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 2640 AND min_dist_hf_bus <= 1320 THEN CASE WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_hf_bus,
                CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 2640 AND (min_dist_hf_bus <= 1320 OR all_bus_count >= 2) THEN CASE WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_bus_combo
            FROM parcel_distances
        )
        SELECT center_geom, area_sqft, parcels_combined, zone_class,

            -- UCLA REDEVELOPMENT LIKELIHOOD FILTER LOGIC
            -- A parcel is only counted if it passes ALL of these structural criteria:
            -- 1. Not a condo (Cook County Class 299)
            -- 2. Not tax-exempt/public land (Cook County Class 8xx)
            -- 3. Not Open Space (Chicago Zone OS/POS)
            -- 4. Fewer than 20 existing units (buying out large buildings is rarely feasible)
            -- 5. The new zoning allows a high enough multiple of existing units to justify demolition costs

            CASE WHEN
                pritzker_capacity >= (COALESCE(tot_existing_units, 1.0) * 2.0) AND
                COALESCE(tot_existing_units, 0) < 20 AND
                zone_class NOT IN ('OS', 'POS', 'PMD') AND
                primary_prop_class NOT LIKE '299%' AND
                primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, pritzker_capacity - current_capacity) ELSE 0 END as new_pritzker,

            CASE WHEN
                cap_true_sb79 >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND
                COALESCE(tot_existing_units, 0) < 20 AND
                zone_class NOT IN ('OS', 'POS', 'PMD') AND
                primary_prop_class NOT LIKE '299%' AND
                primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_true_sb79,

            CASE WHEN
                cap_true_sb79 >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND
                COALESCE(tot_existing_units, 0) < 20 AND
                zone_class NOT IN ('OS', 'POS', 'PMD') AND
                primary_prop_class NOT LIKE '299%' AND
                primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_true_sb79) - current_capacity) ELSE 0 END as tot_true_sb79,

            CASE WHEN cap_train_only >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND COALESCE(tot_existing_units, 0) < 20 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_only,

            CASE WHEN cap_train_only >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND COALESCE(tot_existing_units, 0) < 20 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_only) - current_capacity) ELSE 0 END as tot_train_only,

            CASE WHEN cap_train_and_hf_bus >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND COALESCE(tot_existing_units, 0) < 20 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_hf_bus,

            CASE WHEN cap_train_and_hf_bus >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND COALESCE(tot_existing_units, 0) < 20 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_hf_bus) - current_capacity) ELSE 0 END as tot_train_and_hf_bus,

            CASE WHEN cap_train_and_bus_combo >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND COALESCE(tot_existing_units, 0) < 20 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_bus_combo,

            CASE WHEN cap_train_and_bus_combo >= (COALESCE(tot_existing_units, 1.0) * 3.0) AND COALESCE(tot_existing_units, 0) < 20 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
            THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_bus_combo) - current_capacity) ELSE 0 END as tot_train_and_bus_combo

        FROM parcel_calculations;
    """)

    print("Extracting Neighborhood Aggregates and writing to permanent cache...")
    con.execute("""
        CREATE OR REPLACE TABLE neighborhood_results AS
        SELECT
            n.community as neighborhood_name,
            SUM(pb.new_pritzker) as new_pritzker, SUM(pb.add_true_sb79) as add_true_sb79, SUM(pb.tot_true_sb79) as tot_true_sb79,
            SUM(pb.add_train_only) as add_train_only, SUM(pb.tot_train_only) as tot_train_only,
            SUM(pb.add_train_and_hf_bus) as add_train_and_hf_bus, SUM(pb.tot_train_and_hf_bus) as tot_train_and_hf_bus,
            SUM(pb.add_train_and_bus_combo) as add_train_and_bus_combo, SUM(pb.tot_train_and_bus_combo) as tot_train_and_bus_combo,

            SUM(pb.parcels_combined) as total_parcels,
            SUM(pb.area_sqft) as total_area_sqft,
            SUM(CASE WHEN pb.zone_class NOT LIKE 'RS-1%' AND pb.zone_class NOT LIKE 'RS-2%' AND pb.zone_class NOT LIKE 'RS-3%' THEN pb.parcels_combined ELSE 0 END) as parcels_mf_zoned,
            SUM(CASE WHEN pb.zone_class NOT LIKE 'RS-1%' AND pb.zone_class NOT LIKE 'RS-2%' AND pb.zone_class NOT LIKE 'RS-3%' THEN pb.area_sqft ELSE 0 END) as area_mf_zoned,

            ST_Y(ST_Centroid(n.geom)) as label_lat, ST_X(ST_Centroid(n.geom)) as label_lon
        FROM parcel_base pb JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pb.center_geom, n.geom)
        GROUP BY n.community, n.geom HAVING SUM(pb.tot_true_sb79) > 0 OR SUM(pb.tot_train_and_bus_combo) > 0
    """)
    con.close()
    print("✅ Parcel calculations cached successfully.")
