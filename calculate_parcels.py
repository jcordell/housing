import duckdb
import pandas as pd
import time
from financial_model import get_financial_filter_ctes, CHICAGO_NEW_BUILD_RENTS, DEFAULT_NEW_BUILD_RENT, CHICAGO_SALES_MULTIPLIERS, DEFAULT_SALES_MULTIPLIER

DB_FILE = "sb79_housing.duckdb"

def run_parcel_calculations(full_recalculate=True):
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("PRAGMA enable_progress_bar;")

    if full_recalculate:
        print("\n🚀 Running Full Citywide Spatial Analysis...")

        df_rents = pd.DataFrame(list(CHICAGO_NEW_BUILD_RENTS.items()), columns=['neighborhood_name', 'monthly_rent'])
        con.register('neighborhood_rents_df', df_rents)
        con.execute("CREATE OR REPLACE TEMPORARY TABLE neighborhood_rents AS SELECT * FROM neighborhood_rents_df")

        t0 = time.time()
        print("⏳ [1/4] Isolating citywide parcels and joining neighborhoods...", end="", flush=True)
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE step1_parcels AS
            WITH nbhds AS (SELECT geom, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, UPPER(community) as neighborhood_name FROM ST_Read('neighborhoods.geojson'))
            SELECT p.pin10, ST_Transform(p.geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, n.neighborhood_name
            FROM parcels p
            LEFT JOIN nbhds n ON ST_Intersects(p.geom, n.geom)
            WHERE p.geom IS NOT NULL
        """)
        print(f" ✅ ({time.time() - t0:.1f}s)")

        t0 = time.time()
        print("⏳ [2/4] Joining Zoning and Cook County Assessor data...", end="", flush=True)
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE step2_eligible AS
            WITH target_zones AS (
                SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, zone_class 
                FROM zoning WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'
            ),
            base_parcels AS (
                SELECT p.pin10, p.geom_3435, p.neighborhood_name, ST_Area(p.geom_3435) as area_sqft, z.zone_class
                FROM step1_parcels p
                JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)
            ),
            u_agg AS (
                SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, 
                       ANY_VALUE(CAST("class" AS VARCHAR)) as property_class,
                       COUNT(pin) as tax_pin_count
                FROM assessor_universe GROUP BY 1
            ),
            v_agg AS (SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, SUM(TRY_CAST(certified_bldg AS DOUBLE)) as bldg_value, SUM(TRY_CAST(certified_land AS DOUBLE)) as land_value FROM assessed_values GROUP BY 1),
            rc_agg AS (SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, MAX(TRY_CAST(char_yrblt AS INT)) as char_yrblt, SUM(TRY_CAST(char_bldg_sf AS DOUBLE)) as char_bldg_sf FROM res_characteristics GROUP BY 1),
            pa_agg AS (SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, ANY_VALUE(CAST(prop_address_full AS VARCHAR)) as prop_address FROM parcel_addresses GROUP BY 1)
            
            SELECT bp.pin10, bp.geom_3435, bp.neighborhood_name, bp.area_sqft, bp.zone_class,
                u.property_class as primary_prop_class,
                
                GREATEST(
                    CAST(u.tax_pin_count AS DOUBLE), 
                    CASE 
                        WHEN u.property_class IN ('202','203','204','205','206','207','208','209','210', '234', '278') THEN 1.0
                        WHEN u.property_class = '211' THEN 2.0
                        WHEN u.property_class = '212' THEN 3.0
                        WHEN u.property_class = '213' THEN 5.0
                        WHEN u.property_class = '214' THEN 10.0
                        WHEN u.property_class LIKE '3%' OR u.property_class LIKE '9%' THEN GREATEST(1.0, FLOOR(rc.char_bldg_sf / 1000.0))
                        ELSE 1.0 END
                ) as existing_units,
                
                (2024 - rc.char_yrblt) as building_age,
                rc.char_bldg_sf as existing_sqft,
                pa.prop_address,
                (COALESCE(v.bldg_value, 0.0) / CASE WHEN u.property_class LIKE '2%' THEN 0.10 ELSE 0.25 END) as tot_bldg_value,
                (COALESCE(v.land_value, 0.0) / CASE WHEN u.property_class LIKE '2%' THEN 0.10 ELSE 0.25 END) as tot_land_value
            FROM base_parcels bp
            LEFT JOIN u_agg u ON bp.pin10 = u.pin10
            LEFT JOIN v_agg v ON bp.pin10 = v.pin10
            LEFT JOIN rc_agg rc ON bp.pin10 = rc.pin10
            LEFT JOIN pa_agg pa ON bp.pin10 = pa.pin10
        """)
        print(f" ✅ ({time.time() - t0:.1f}s)")

        t0 = time.time()
        print("⏳ [3/4] Calculating Transit Distances (Spatial R-Tree Indexing)...", end="", flush=True)
        con.execute("""
            CREATE OR REPLACE TABLE parcel_base_unfiltered AS
            WITH 
            projected_transit AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM transit_stops),
            projected_bus_all AS (SELECT CAST(route AS VARCHAR) as route, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM bus_routes),
            projected_bus_hf AS (SELECT geom_3435 FROM projected_bus_all WHERE route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')),
            projected_bus_brt AS (SELECT geom_3435 FROM projected_bus_all WHERE route = 'J14'),
            
            train_1320 AS (SELECT DISTINCT ep.pin10 FROM step2_eligible ep JOIN projected_transit t ON ST_Intersects(ep.geom_3435, ST_Buffer(t.geom_3435, 1320))),
            train_2640 AS (SELECT DISTINCT ep.pin10 FROM step2_eligible ep JOIN projected_transit t ON ST_Intersects(ep.geom_3435, ST_Buffer(t.geom_3435, 2640))),
            brt_1320 AS (SELECT DISTINCT ep.pin10 FROM step2_eligible ep JOIN projected_bus_brt b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320))),
            brt_2640 AS (SELECT DISTINCT ep.pin10 FROM step2_eligible ep JOIN projected_bus_brt b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 2640))),
            hf_1320 AS (SELECT DISTINCT ep.pin10 FROM step2_eligible ep JOIN projected_bus_hf b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320))),
            
            bus_counts AS (
                SELECT ep.pin10, 
                       COUNT(DISTINCT b.route) as all_bus_count, 
                       COUNT(DISTINCT CASE WHEN b.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b.route END) as hf_bus_count
                FROM step2_eligible ep 
                JOIN projected_bus_all b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320))
                GROUP BY ep.pin10
            )
            
            SELECT ep.*, 
                   CASE WHEN t13.pin10 IS NOT NULL THEN true ELSE false END as is_train_1320,
                   CASE WHEN t26.pin10 IS NOT NULL THEN true ELSE false END as is_train_2640,
                   CASE WHEN b13.pin10 IS NOT NULL THEN true ELSE false END as is_brt_1320,
                   CASE WHEN b26.pin10 IS NOT NULL THEN true ELSE false END as is_brt_2640,
                   CASE WHEN h13.pin10 IS NOT NULL THEN true ELSE false END as is_hf_1320,
                   COALESCE(bc.all_bus_count, 0) as all_bus_count,
                   COALESCE(bc.hf_bus_count, 0) as hf_bus_count
            FROM step2_eligible ep
            LEFT JOIN train_1320 t13 ON ep.pin10 = t13.pin10
            LEFT JOIN train_2640 t26 ON ep.pin10 = t26.pin10
            LEFT JOIN brt_1320 b13 ON ep.pin10 = b13.pin10
            LEFT JOIN brt_2640 b26 ON ep.pin10 = b26.pin10
            LEFT JOIN hf_1320 h13 ON ep.pin10 = h13.pin10
            LEFT JOIN bus_counts bc ON ep.pin10 = bc.pin10
        """)
        print(f" ✅ ({time.time() - t0:.1f}s)")

    t0 = time.time()
    try:
        con.execute("SELECT 1 FROM parcel_sales LIMIT 1")
        has_sales_data = True
    except duckdb.duckdb.CatalogException:
        has_sales_data = False

    if has_sales_data:
        print("⏳ [4/4] Calculating Dynamic Sales Ratios...", end="", flush=True)
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE step4_sales_ratio AS
            WITH clean_sales AS (
                SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, 
                       TRY_CAST(sale_price AS DOUBLE) as sale_price
                FROM parcel_sales
                WHERE TRY_CAST(sale_price AS DOUBLE) > 20000 
            ),
            valid_ratios AS (
                SELECT ep.neighborhood_name,
                       (s.sale_price / (ep.tot_bldg_value + ep.tot_land_value)) as ratio
                FROM parcel_base_unfiltered ep
                JOIN clean_sales s ON ep.pin10 = s.pin10
                WHERE (ep.tot_bldg_value + ep.tot_land_value) > 20000
            )
            SELECT neighborhood_name,
                   MEDIAN(ratio) as market_correction_multiplier
            FROM valid_ratios
            WHERE ratio BETWEEN 0.5 AND 2.5 -- Filter out $1 transfers or data errors
            GROUP BY neighborhood_name;
        """)
    else:
        print("⏳ [4/4] API down. Falling back to hardcoded Market Correction Multipliers...", end="", flush=True)
        df_mults = pd.DataFrame(list(CHICAGO_SALES_MULTIPLIERS.items()), columns=['neighborhood_name', 'fallback_mult'])
        con.register('fallback_mults_df', df_mults)
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE step4_sales_ratio AS 
            SELECT neighborhood_name, fallback_mult as market_correction_multiplier 
            FROM fallback_mults_df
        """)
    print(f" ✅ ({time.time() - t0:.1f}s)")

    print("⏳ Executing Real Estate Pro Forma filters...", end="", flush=True)
    financial_ctes = get_financial_filter_ctes("parcel_calculations")

    con.execute(f"""
        CREATE OR REPLACE TABLE neighborhood_results AS
        WITH parcel_calculations AS (
            SELECT pd.geom_3435 as center_geom, pd.neighborhood_name, pd.area_sqft, pd.zone_class, 1 as parcels_combined, pd.existing_units as tot_existing_units, pd.primary_prop_class, pd.tot_bldg_value, pd.tot_land_value, pd.building_age, pd.existing_sqft, pd.prop_address,
                COALESCE(r.monthly_rent, {DEFAULT_NEW_BUILD_RENT}) as local_rent,
                COALESCE(nsr.market_correction_multiplier, {DEFAULT_SALES_MULTIPLIER}) as market_correction_multiplier,
                GREATEST(1, CASE WHEN pd.zone_class LIKE 'RS-1%' OR pd.zone_class LIKE 'RS-2%' THEN FLOOR(pd.area_sqft / 5000) WHEN pd.zone_class LIKE 'RS-3%' THEN FLOOR(pd.area_sqft / 2500) WHEN pd.zone_class LIKE 'RT-3.5%' THEN FLOOR(pd.area_sqft / 1250) WHEN pd.zone_class LIKE 'RT-4%' THEN FLOOR(pd.area_sqft / 1000) WHEN pd.zone_class LIKE 'RM-4.5%' OR pd.zone_class LIKE 'RM-5%' THEN FLOOR(pd.area_sqft / 400) WHEN pd.zone_class LIKE 'RM-6%' OR pd.zone_class LIKE 'RM-6.5%' THEN FLOOR(pd.area_sqft / 200) WHEN pd.zone_class LIKE '%-1' THEN FLOOR(pd.area_sqft / 1000) WHEN pd.zone_class LIKE '%-2' OR pd.zone_class LIKE '%-3' THEN FLOOR(pd.area_sqft / 400) WHEN pd.zone_class LIKE '%-5' OR pd.zone_class LIKE '%-6' THEN FLOOR(pd.area_sqft / 200) ELSE FLOOR(pd.area_sqft / 1000) END) as current_capacity,
                CASE WHEN pd.zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN CASE WHEN pd.area_sqft < 2500 THEN 1 WHEN pd.area_sqft < 5000 THEN 4 WHEN pd.area_sqft < 7500 THEN 6 ELSE 8 END ELSE 0 END as pritzker_capacity,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) WHEN pd.is_train_2640 OR pd.is_brt_1320 OR pd.hf_bus_count >= 2 THEN FLOOR((pd.area_sqft / 43560.0) * 100) WHEN pd.is_brt_2640 THEN FLOOR((pd.area_sqft / 43560.0) * 80) ELSE 0 END as cap_true_sb79,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) WHEN pd.is_train_2640 THEN FLOOR((pd.area_sqft / 43560.0) * 100) ELSE 0 END as cap_train_only,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_2640 AND pd.is_hf_1320 THEN CASE WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) ELSE FLOOR((pd.area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_hf_bus,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_2640 AND (pd.is_hf_1320 OR pd.all_bus_count >= 2) THEN CASE WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) ELSE FLOOR((pd.area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_bus_combo
            FROM parcel_base_unfiltered pd
            LEFT JOIN neighborhood_rents r ON pd.neighborhood_name = r.neighborhood_name
            LEFT JOIN step4_sales_ratio nsr ON pd.neighborhood_name = nsr.neighborhood_name
        ),
        {financial_ctes}
        SELECT 
            neighborhood_name, 
            SUM(fp.feasible
