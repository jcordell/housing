import duckdb
import pandas as pd
import time

CHICAGO_NEW_BUILD_RENTS = {
    'LINCOLN PARK': 2800.0, 'NEAR NORTH SIDE': 2900.0, 'LOOP': 2900.0, 'NEAR WEST SIDE': 2800.0,
    'LAKE VIEW': 2400.0, 'WEST TOWN': 2500.0, 'LOGAN SQUARE': 2300.0, 'NORTH CENTER': 2300.0,
    'LINCOLN SQUARE': 2100.0, 'UPTOWN': 2000.0, 'EDGEWATER': 1900.0, 'AVONDALE': 1900.0,
    'HYDE PARK': 2100.0, 'BRIDGEPORT': 1800.0, 'PORTAGE PARK': 1700.0, 'ASHBURN': 1500.0,
    'AUSTIN': 1400.0, 'ENGLEWOOD': 1300.0, 'WASHINGTON PARK': 1500.0
}
DEFAULT_NEW_BUILD_RENT = 1600.0

CHICAGO_SALES_MULTIPLIERS = {
    'LINCOLN PARK': 1.65, 'LAKE VIEW': 1.55, 'NEAR NORTH SIDE': 1.60,
    'WEST TOWN': 1.55, 'LOGAN SQUARE': 1.50, 'AUSTIN': 1.25, 'ASHBURN': 1.20,
}
DEFAULT_SALES_MULTIPLIER = 1.40

def get_financial_filter_ctes(source_table_name):
    return f"""
        filtered_parcels AS (
            SELECT center_geom, area_sqft, parcels_combined, zone_class, neighborhood_name, prop_address,
                local_rent, value_per_new_unit, acquisition_cost, existing_units, building_age, existing_sqft,
                current_capacity, primary_prop_class, tot_bldg_value, tot_land_value, market_correction_multiplier,
                cost_per_unit_low_density, cost_per_unit_high_density, target_profit_margin,

                (current_capacity >= (GREATEST(existing_units, 1.0) * 2.0)) as pass_unit_mult,
                ((current_capacity * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25)) as pass_sqft_mult,
                ((existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560) as pass_lot_density,
                (existing_units < 40) as pass_max_units,
                (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) as pass_age_value,
                (zone_class NOT IN ('OS', 'POS', 'PMD')) as pass_zoning_class,
                (primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX') as pass_prop_class,
                ((current_capacity * value_per_new_unit) > (acquisition_cost + (current_capacity * CASE WHEN current_capacity > 6 THEN cost_per_unit_high_density ELSE cost_per_unit_low_density END)) * target_profit_margin) as pass_financial_existing,

                CASE WHEN
                    current_capacity >= (GREATEST(existing_units, 1.0) * 2.0) AND
                    (current_capacity * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND
                    existing_units < 40 AND
                    (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND
                    zone_class NOT IN ('OS', 'POS', 'PMD') AND
                    primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (current_capacity * value_per_new_unit) > (acquisition_cost + (current_capacity * CASE WHEN current_capacity > 6 THEN cost_per_unit_high_density ELSE cost_per_unit_low_density END)) * target_profit_margin
                THEN GREATEST(0, current_capacity - existing_units) ELSE 0 END as feasible_existing,

                CASE WHEN
                    pritzker_capacity >= (GREATEST(existing_units, 1.0) * 2.0) AND
                    (pritzker_capacity * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (pritzker_capacity * value_per_new_unit) > (acquisition_cost + (pritzker_capacity * cost_per_unit_low_density)) * target_profit_margin
                THEN GREATEST(0, pritzker_capacity - current_capacity) ELSE 0 END as new_pritzker,

                CASE WHEN
                    cap_true_sb79 >= (GREATEST(existing_units, 1.0) * 2.0) AND
                    (cap_true_sb79 * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_true_sb79,

                CASE WHEN
                    cap_true_sb79 >= (GREATEST(existing_units, 1.0) * 2.0) AND
                    (cap_true_sb79 * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_true_sb79) - current_capacity) ELSE 0 END as tot_true_sb79,

                CASE WHEN cap_train_only >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_only * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_only,

                CASE WHEN cap_train_only >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_only * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_only) - current_capacity) ELSE 0 END as tot_train_only,

                CASE WHEN cap_train_and_hf_bus >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_hf_bus * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_hf_bus,

                CASE WHEN cap_train_and_hf_bus >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_hf_bus * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_hf_bus) - current_capacity) ELSE 0 END as tot_train_and_hf_bus,

                CASE WHEN cap_train_and_bus_combo >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_bus_combo * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_bus_combo,

                CASE WHEN cap_train_and_bus_combo >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_bus_combo * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_bus_combo) - current_capacity) ELSE 0 END as tot_train_and_bus_combo,

                parcels_combined, area_sqft,

                CASE WHEN zone_class LIKE 'RM-%' OR zone_class LIKE 'RT-%' THEN parcels_combined ELSE 0 END as parcels_mf_zoned,
                CASE WHEN zone_class LIKE 'RM-%' OR zone_class LIKE 'RT-%' THEN area_sqft ELSE 0 END as area_mf_zoned

            FROM {source_table_name}
        )
    """

def run_spatial_pipeline(con, is_sandbox=False):
    df_rents = pd.DataFrame(list(CHICAGO_NEW_BUILD_RENTS.items()), columns=['neighborhood_name', 'monthly_rent'])
    con.register('neighborhood_rents_df', df_rents)
    con.execute("CREATE OR REPLACE TEMPORARY TABLE neighborhood_rents AS SELECT * FROM neighborhood_rents_df")

    t0 = time.time()
    if is_sandbox:
        print("⏳ [1/5] Isolating parcels for the 4 test neighborhoods...", end="", flush=True)
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE step1_parcels AS
            WITH test_nbhds AS (
                SELECT geom, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, UPPER(community) as neighborhood_name
                FROM ST_Read('neighborhoods.geojson')
                WHERE UPPER(community) IN ('LINCOLN PARK', 'LAKE VIEW', 'ASHBURN', 'AUSTIN')
            )
            SELECT p.pin10, ST_Transform(p.geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, tn.neighborhood_name
            FROM parcels p
            JOIN test_nbhds tn ON ST_Intersects(p.geom, tn.geom)
            WHERE p.geom IS NOT NULL
        """)
    else:
        print("⏳ [1/5] Isolating citywide parcels and joining neighborhoods...", end="", flush=True)
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
    print("⏳ [2/5] Joining Zoning and Cook County Assessor data...", end="", flush=True)
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
    print("⏳ [3/5] Calculating Transit Distances...", end="", flush=True)
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE step3_distances AS
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
            SELECT ep.pin10, COUNT(DISTINCT b.route) as all_bus_count, COUNT(DISTINCT CASE WHEN b.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b.route END) as hf_bus_count
            FROM step2_eligible ep JOIN projected_bus_all b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320)) GROUP BY ep.pin10
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
        print("⏳ [4/5] Calculating Property-Type Stratified Sales Ratios...", end="", flush=True)
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
                       CASE
                           WHEN ep.primary_prop_class IN ('211', '212', '213', '214') THEN 'MULTI_FAMILY'
                           WHEN ep.primary_prop_class IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278') THEN 'SFH'
                           WHEN ep.primary_prop_class LIKE '3%' OR ep.primary_prop_class LIKE '5%' THEN 'COMMERCIAL'
                           ELSE 'OTHER'
                       END as prop_category,
                       (s.sale_price / (ep.tot_bldg_value + ep.tot_land_value)) as ratio
                FROM step3_distances ep
                JOIN clean_sales s ON ep.pin10 = s.pin10
                WHERE (ep.tot_bldg_value + ep.tot_land_value) > 20000
            ),
            bucket_medians AS (
                SELECT neighborhood_name, prop_category, MEDIAN(ratio) as bucket_multiplier
                FROM valid_ratios
                WHERE ratio BETWEEN 0.5 AND 3.5
                GROUP BY neighborhood_name, prop_category
            ),
            neighborhood_medians AS (
                SELECT neighborhood_name, MEDIAN(ratio) as neighborhood_multiplier
                FROM valid_ratios
                WHERE ratio BETWEEN 0.5 AND 3.5
                GROUP BY neighborhood_name
            ),
            all_categories AS (
                SELECT 'MULTI_FAMILY' as prop_category UNION ALL
                SELECT 'SFH' UNION ALL
                SELECT 'COMMERCIAL' UNION ALL
                SELECT 'OTHER'
            )
            SELECT n.neighborhood_name,
                   ac.prop_category,
                   COALESCE(b.bucket_multiplier, n.neighborhood_multiplier) as market_correction_multiplier
            FROM neighborhood_medians n
            CROSS JOIN all_categories ac
            LEFT JOIN bucket_medians b ON n.neighborhood_name = b.neighborhood_name AND ac.prop_category = b.prop_category;
        """)
    else:
        print("⏳ [4/5] API down. Falling back to hardcoded Market Correction Multipliers...", end="", flush=True)
        df_mults = pd.DataFrame(list(CHICAGO_SALES_MULTIPLIERS.items()), columns=['neighborhood_name', 'fallback_mult'])
        con.register('fallback_mults_df', df_mults)
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE step4_sales_ratio AS
            SELECT neighborhood_name, prop_category, fallback_mult as market_correction_multiplier
            FROM fallback_mults_df
            CROSS JOIN (SELECT 'MULTI_FAMILY' as prop_category UNION SELECT 'SFH' UNION SELECT 'COMMERCIAL' UNION SELECT 'OTHER') cats
        """)
    print(f" ✅ ({time.time() - t0:.1f}s)")

    t0 = time.time()
    print("⏳ [5/5] Executing Real Estate Pro Forma equations...", end="", flush=True)

    con.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE step5_pro_forma_base AS
        SELECT pd.geom_3435 as center_geom, pd.neighborhood_name, pd.area_sqft, pd.zone_class, 1 as parcels_combined,
            COALESCE(pd.existing_units, 0.0) as existing_units,
            COALESCE(pd.primary_prop_class, 'UNKNOWN') as primary_prop_class,
            COALESCE(pd.tot_bldg_value, 0.0) as tot_bldg_value,
            COALESCE(pd.tot_land_value, 0.0) as tot_land_value,
            COALESCE(pd.building_age, 0) as building_age,
            COALESCE(pd.existing_sqft, 0.0) as existing_sqft,
            pd.prop_address,

            COALESCE(r.monthly_rent, {DEFAULT_NEW_BUILD_RENT}) as local_rent,
            ((COALESCE(r.monthly_rent, {DEFAULT_NEW_BUILD_RENT}) * 12.0) / 0.055) as value_per_new_unit,

            COALESCE(nsr.market_correction_multiplier, {DEFAULT_SALES_MULTIPLIER}) as market_correction_multiplier,
            GREATEST((COALESCE(pd.tot_bldg_value, 0.0) + COALESCE(pd.tot_land_value, 0.0)) * COALESCE(nsr.market_correction_multiplier, {DEFAULT_SALES_MULTIPLIER}), 10000.0) as acquisition_cost,

            CASE WHEN pd.neighborhood_name IN ('LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'LOOP', 'NEAR WEST SIDE')
                 THEN 300000.0 ELSE 240000.0 END as cost_per_unit_low_density,

            CASE WHEN pd.neighborhood_name IN ('LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'LOOP', 'NEAR WEST SIDE')
                 THEN 420000.0 ELSE 336000.0 END as cost_per_unit_high_density,

            1.15 as target_profit_margin,

            LEAST(150, GREATEST(1, CASE WHEN pd.zone_class LIKE 'RS-1%' OR pd.zone_class LIKE 'RS-2%' THEN FLOOR(pd.area_sqft / 5000) WHEN pd.zone_class LIKE 'RS-3%' THEN FLOOR(pd.area_sqft / 2500) WHEN pd.zone_class LIKE 'RT-3.5%' THEN FLOOR(pd.area_sqft / 1250) WHEN pd.zone_class LIKE 'RT-4%' THEN FLOOR(pd.area_sqft / 1000) WHEN pd.zone_class LIKE 'RM-4.5%' OR pd.zone_class LIKE 'RM-5%' THEN FLOOR(pd.area_sqft / 400) WHEN pd.zone_class LIKE 'RM-6%' OR pd.zone_class LIKE 'RM-6.5%' THEN FLOOR(pd.area_sqft / 200) WHEN pd.zone_class LIKE '%-1' THEN FLOOR(pd.area_sqft / 1000) WHEN pd.zone_class LIKE '%-2' OR pd.zone_class LIKE '%-3' THEN FLOOR(pd.area_sqft / 400) WHEN pd.zone_class LIKE '%-5' OR pd.zone_class LIKE '%-6' THEN FLOOR(pd.area_sqft / 200) ELSE FLOOR(pd.area_sqft / 1000) END)) as current_capacity,
            LEAST(150, CASE WHEN pd.zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN CASE WHEN pd.area_sqft < 2500 THEN 1 WHEN pd.area_sqft < 5000 THEN 4 WHEN pd.area_sqft < 7500 THEN 6 ELSE 8 END ELSE 0 END) as pritzker_capacity,
            LEAST(150, CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) WHEN pd.is_train_2640 OR pd.is_brt_1320 OR pd.hf_bus_count >= 2 THEN FLOOR((pd.area_sqft / 43560.0) * 100) WHEN pd.is_brt_2640 THEN FLOOR((pd.area_sqft / 43560.0) * 80) ELSE 0 END) as cap_true_sb79,
            LEAST(150, CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) WHEN pd.is_train_2640 THEN FLOOR((pd.area_sqft / 43560.0) * 100) ELSE 0 END) as cap_train_only,
            LEAST(150, CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_2640 AND pd.is_hf_1320 THEN CASE WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) ELSE FLOOR((pd.area_sqft / 43560.0) * 100) END ELSE 0 END) as cap_train_and_hf_bus,
            LEAST(150, CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.is_train_2640 AND (pd.is_hf_1320 OR pd.all_bus_count >= 2) THEN CASE WHEN pd.is_train_1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) ELSE FLOOR((pd.area_sqft / 43560.0) * 100) END ELSE 0 END) as cap_train_and_bus_combo

        FROM step3_distances pd
        LEFT JOIN neighborhood_rents r ON pd.neighborhood_name = r.neighborhood_name
        LEFT JOIN step4_sales_ratio nsr
            ON pd.neighborhood_name = nsr.neighborhood_name
            AND nsr.prop_category = CASE
                WHEN pd.primary_prop_class IN ('211', '212', '213', '214') THEN 'MULTI_FAMILY'
                WHEN pd.primary_prop_class IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278') THEN 'SFH'
                WHEN pd.primary_prop_class LIKE '3%' OR pd.primary_prop_class LIKE '5%' THEN 'COMMERCIAL'
                ELSE 'OTHER'
            END
    """)

    financial_ctes = get_financial_filter_ctes(source_table_name="step5_pro_forma_base")
    con.execute(f"CREATE OR REPLACE TEMPORARY TABLE step5_pro_forma AS WITH {financial_ctes} SELECT * FROM filtered_parcels")
    print(f" ✅ ({time.time() - t0:.1f}s)")
