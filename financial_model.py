import duckdb
import pandas as pd
import time
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

CHICAGO_SALES_MULTIPLIERS = {
    'LINCOLN PARK': 1.65, 'LAKE VIEW': 1.55, 'NEAR NORTH SIDE': 1.60,
    'WEST TOWN': 1.55, 'LOGAN SQUARE': 1.50, 'AUSTIN': 1.25, 'ASHBURN': 1.20,
}
DEFAULT_SALES_MULTIPLIER = 1.40

def get_financial_filter_ctes(source_table_name, eco):
    margin = eco.get('target_profit_margin', 1.15)
    far_curr = eco.get('far_current', 1.2)
    far_pritzker = eco.get('far_pritzker', 1.5)
    far_sb79 = eco.get('far_sb79', 3.0)
    far_train = eco.get('far_train', 3.0)
    far_hf = eco.get('far_hf', 2.5)
    far_combo = eco.get('far_combo', 2.5)
    eff = eco.get('efficiency_factor', 0.82)
    min_unit = eco.get('min_unit_size_sqft', 750.0)

    return f"""
        base_capacities AS (
            SELECT *,
                CASE 
                    WHEN neighborhood_name IN ('ENGLEWOOD', 'WEST ENGLEWOOD', 'WOODLAWN', 'WASHINGTON PARK', 
                                               'CHATHAM', 'AUBURN GRESHAM', 'SOUTH SHORE', 'ROSELAND', 
                                               'PULLMAN', 'GREATER GRAND CROSSING', 'BRONZEVILLE', 'SOUTH CHICAGO')
                         AND CAST(primary_prop_class AS VARCHAR) IN ('100', '241', '242')
                    THEN 1.0
                    ELSE GREATEST(
                        (tot_bldg_value + tot_land_value) * market_correction_multiplier,
                        area_sqft * acq_cost_floor_per_sqft
                    )
                END as acq_cost,

                LEAST(150, FLOOR(area_sqft / 400), GREATEST(1, CASE 
                    WHEN zone_class LIKE 'RS-1%' OR zone_class LIKE 'RS-2%' THEN FLOOR(area_sqft / 5000)
                    WHEN zone_class LIKE 'RS-3%' THEN FLOOR(area_sqft / 2500)
                    WHEN zone_class LIKE 'RT-3.5%' THEN FLOOR(area_sqft / 1250)
                    WHEN zone_class LIKE 'RT-4%' THEN FLOOR(area_sqft / 1000)
                    WHEN zone_class LIKE 'RM-4.5%' OR zone_class LIKE 'RM-5%' THEN FLOOR(area_sqft / 400)
                    WHEN zone_class LIKE 'RM-6%' OR zone_class LIKE 'RM-6.5%' THEN FLOOR(area_sqft / 200)
                    WHEN zone_class LIKE '%-1' THEN FLOOR(area_sqft / 1000)
                    WHEN zone_class LIKE '%-2' OR zone_class LIKE '%-3' THEN FLOOR(area_sqft / 400)
                    WHEN zone_class LIKE '%-5' OR zone_class LIKE '%-6' THEN FLOOR(area_sqft / 200)
                    ELSE FLOOR(area_sqft / 1000) END)) as cap_curr_raw,

                LEAST(150, FLOOR(area_sqft / 400), CASE WHEN zone_class SIMILAR TO '(RS|RT|RM).*' THEN CASE WHEN area_sqft < 2500 THEN 1 WHEN area_sqft < 5000 THEN 4 WHEN area_sqft < 7500 THEN 6 ELSE 8 END ELSE 0 END) as cap_pritzker_raw,
                LEAST(150, FLOOR(area_sqft / 400), CASE WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120) WHEN is_train_2640 OR is_brt_1320 OR hf_bus_count >= 2 THEN FLOOR((area_sqft / 43560.0) * 100) WHEN is_brt_2640 THEN FLOOR((area_sqft / 43560.0) * 80) ELSE 0 END) as cap_sb79_raw,
                LEAST(150, FLOOR(area_sqft / 400), CASE WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120) WHEN is_train_2640 THEN FLOOR((area_sqft / 43560.0) * 100) ELSE 0 END) as cap_train_raw,
                LEAST(150, FLOOR(area_sqft / 400), CASE WHEN is_train_2640 AND is_hf_1320 THEN CASE WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END ELSE 0 END) as cap_hf_raw,
                LEAST(150, FLOOR(area_sqft / 400), CASE WHEN is_train_2640 AND (is_hf_1320 OR all_bus_count >= 2) THEN CASE WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END ELSE 0 END) as cap_combo_raw
            FROM {source_table_name}
        ),
        capacities AS (
            SELECT *,
                cap_curr_raw as cap_curr,
                GREATEST(cap_curr_raw, cap_pritzker_raw) as cap_pritzker,
                GREATEST(cap_curr_raw, cap_pritzker_raw, cap_sb79_raw) as cap_sb79,
                GREATEST(cap_curr_raw, cap_pritzker_raw, cap_train_raw) as cap_train_only,
                GREATEST(cap_curr_raw, cap_pritzker_raw, cap_hf_raw) as cap_train_hf,
                GREATEST(cap_curr_raw, cap_pritzker_raw, cap_combo_raw) as cap_train_combo,
                
                (existing_units < 40) as pass_max_units,
                (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 250000)) as pass_age_value,
                (zone_class NOT IN ('OS', 'POS', 'PMD')) as pass_zoning_class,
                (
                    primary_prop_class IS NOT NULL 
                    AND primary_prop_class != 'UNKNOWN'
                    AND primary_prop_class != 'EX'      -- Exclude Exempt (Churches/Gov)
                    AND primary_prop_class != '299'     -- EXCLUDE CONDOS (Primary)
                    AND primary_prop_class NOT LIKE '299%' -- EXCLUDE CONDOS (Secondary)
                    AND primary_prop_class NOT LIKE '599%' -- Exclude Commercial Condos
                    AND primary_prop_class NOT LIKE '8%'   -- Exclude most Large/Specialized
                    AND primary_prop_class != '0'       -- Exclude Rail/Public Right of Way
                ) as pass_prop_class,
                ((tot_bldg_value + tot_land_value) >= 1000) as pass_min_value,
                ((existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560) as pass_lot_density
            FROM base_capacities
        ),
        financial_metrics AS (
            SELECT *,
                (area_sqft * {far_curr}) as gsf_curr,
                (area_sqft * {far_pritzker}) as gsf_pritzker,
                (area_sqft * {far_sb79}) as gsf_sb79,
                (area_sqft * {far_train}) as gsf_train,
                (area_sqft * {far_hf}) as gsf_hf,
                (area_sqft * {far_combo}) as gsf_combo,

                -- Current Rules (Double Stair Penalty)
                ((area_sqft * {far_curr}) * CASE WHEN cap_curr <= 2 THEN 0.90
                         WHEN cap_curr <= 4 THEN 0.75
                         WHEN cap_curr <= 9 THEN 0.78
                         WHEN cap_curr <= 19 THEN 0.80
                         ELSE 0.82 END
                ) as nra_curr,

                -- Pritzker & SB79 Rules (Single Stair Bump)
                ((area_sqft * {far_pritzker}) * CASE WHEN cap_pritzker <= 2 THEN 0.90
                         WHEN cap_pritzker <= 6 THEN 0.87
                         WHEN cap_pritzker <= 15 THEN 0.85
                         ELSE 0.82 END
                ) as nra_pritzker,

                ((area_sqft * {far_sb79}) * CASE WHEN cap_sb79 <= 2 THEN 0.90
                         WHEN cap_sb79 <= 6 THEN 0.87
                         WHEN cap_sb79 <= 15 THEN 0.85
                         ELSE 0.82 END
                ) as nra_sb79,

                ((area_sqft * {far_train}) * CASE WHEN cap_train_only <= 2 THEN 0.90
                         WHEN cap_train_only <= 6 THEN 0.87
                         WHEN cap_train_only <= 15 THEN 0.85
                         ELSE 0.82 END
                ) as nra_train,

                ((area_sqft * {far_hf}) * CASE WHEN cap_train_hf <= 2 THEN 0.90
                         WHEN cap_train_hf <= 6 THEN 0.87
                         WHEN cap_train_hf <= 15 THEN 0.85
                         ELSE 0.82 END
                ) as nra_hf,

                ((area_sqft * {far_combo}) * CASE WHEN cap_train_combo <= 2 THEN 0.90
                         WHEN cap_train_combo <= 6 THEN 0.87
                         WHEN cap_train_combo <= 15 THEN 0.85
                         ELSE 0.82 END
                ) as nra_combo
            FROM capacities
        ),
        revenue_metrics AS (
            SELECT *,
                (nra_curr * condo_price_per_sqft) as rev_curr,
                (nra_pritzker * condo_price_per_sqft) as rev_pritzker,
                (nra_sb79 * condo_price_per_sqft) as rev_sb79,
                (nra_train * condo_price_per_sqft) as rev_train,
                (nra_hf * condo_price_per_sqft) as rev_hf,
                (nra_combo * condo_price_per_sqft) as rev_combo,
                
                LEAST(cap_curr, FLOOR(nra_curr / {min_unit})) as final_cap_curr,
                LEAST(cap_pritzker, FLOOR(nra_pritzker / {min_unit})) as final_cap_pritzker,
                LEAST(cap_sb79, FLOOR(nra_sb79 / {min_unit})) as final_cap_sb79,
                LEAST(cap_train_only, FLOOR(nra_train / {min_unit})) as final_cap_train,
                LEAST(cap_train_hf, FLOOR(nra_hf / {min_unit})) as final_cap_hf,
                LEAST(cap_train_combo, FLOOR(nra_combo / {min_unit})) as final_cap_combo
            FROM financial_metrics
        ),
        profit_eval AS (
            SELECT *,
                acq_cost + (gsf_curr * const_cost_per_sqft) as cost_curr,
                acq_cost + (gsf_pritzker * const_cost_per_sqft) as cost_pritzker,
                acq_cost + (gsf_sb79 * const_cost_per_sqft) as cost_sb79,
                acq_cost + (gsf_train * const_cost_per_sqft) as cost_train,
                acq_cost + (gsf_hf * const_cost_per_sqft) as cost_hf,
                acq_cost + (gsf_combo * const_cost_per_sqft) as cost_combo
            FROM revenue_metrics
        ),
        feasibility_check AS (
            SELECT *,
                rev_curr - cost_curr as profit_curr,
                rev_pritzker - cost_pritzker as profit_pritzker,
                rev_sb79 - cost_sb79 as profit_sb79,
                rev_train - cost_train as profit_train,
                rev_hf - cost_hf as profit_hf,
                rev_combo - cost_combo as profit_combo,
                
                (rev_curr > (cost_curr * {margin})) as feas_curr,
                (rev_pritzker > (cost_pritzker * {margin})) as feas_pritzker,
                (rev_sb79 > (cost_sb79 * {margin})) as feas_sb79,
                (rev_train > (cost_train * {margin})) as feas_train,
                (rev_hf > (cost_hf * {margin})) as feas_hf,
                (rev_combo > (cost_combo * {margin})) as feas_combo
            FROM profit_eval
        ),
        hbu_waterfall AS (
            SELECT *,
                CASE WHEN feas_curr AND final_cap_curr > existing_units AND final_cap_curr >= (GREATEST(existing_units, 1.0) * 2.0) THEN final_cap_curr ELSE 0 END as yield_curr,
                CASE WHEN feas_curr AND final_cap_curr > existing_units AND final_cap_curr >= (GREATEST(existing_units, 1.0) * 2.0) THEN profit_curr ELSE 0 END as max_profit_curr,
                
                (cost_curr / NULLIF(final_cap_curr, 0)) as cpu_current,
                (cost_pritzker / NULLIF(final_cap_pritzker, 0)) as cpu_pritzker,
                (cost_sb79 / NULLIF(final_cap_sb79, 0)) as cpu_sb79
            FROM feasibility_check
        ),
        ratchet_application AS (
            SELECT *,
                CASE WHEN feas_pritzker AND final_cap_pritzker > existing_units AND final_cap_pritzker >= (GREATEST(existing_units, 1.0) * 2.0) AND profit_pritzker > max_profit_curr THEN final_cap_pritzker ELSE yield_curr END as yield_pritzker,
                CASE WHEN feas_pritzker AND final_cap_pritzker > existing_units AND final_cap_pritzker >= (GREATEST(existing_units, 1.0) * 2.0) AND profit_pritzker > max_profit_curr THEN profit_pritzker ELSE max_profit_curr END as max_profit_pritzker
            FROM hbu_waterfall
        ),
        final_yields AS (
            SELECT *,
                CASE WHEN feas_sb79 AND final_cap_sb79 > existing_units AND final_cap_sb79 >= (GREATEST(existing_units, 1.0) * 2.0) AND profit_sb79 > max_profit_pritzker THEN final_cap_sb79 ELSE yield_pritzker END as yield_sb79,
                CASE WHEN feas_train AND final_cap_train > existing_units AND final_cap_train >= (GREATEST(existing_units, 1.0) * 2.0) AND profit_train > max_profit_pritzker THEN final_cap_train ELSE yield_pritzker END as yield_train,
                CASE WHEN feas_hf AND final_cap_hf > existing_units AND final_cap_hf >= (GREATEST(existing_units, 1.0) * 2.0) AND profit_hf > max_profit_pritzker THEN final_cap_hf ELSE yield_pritzker END as yield_hf,
                CASE WHEN feas_combo AND final_cap_combo > existing_units AND final_cap_combo >= (GREATEST(existing_units, 1.0) * 2.0) AND profit_combo > max_profit_pritzker THEN final_cap_combo ELSE yield_pritzker END as yield_combo
            FROM ratchet_application
        ),
        filtered_parcels AS (
            SELECT 
                center_geom, area_sqft, parcels_combined, zone_class, neighborhood_name, prop_address,
                condo_price_per_sqft, acq_cost as acquisition_cost, existing_units, building_age, existing_sqft,
                final_cap_curr as current_capacity, primary_prop_class, tot_bldg_value, tot_land_value, market_correction_multiplier,
                cpu_current, cpu_pritzker, cpu_sb79,
                rev_curr, rev_pritzker, rev_sb79,
                cost_curr, cost_pritzker, cost_sb79,
                
                pass_max_units, pass_age_value, pass_zoning_class, pass_prop_class, pass_min_value, pass_lot_density,
                
                (yield_curr >= (GREATEST(existing_units, 1.0) * 2.0)) as pass_unit_mult,
                (gsf_curr >= (GREATEST(existing_sqft, 1.0) * 1.25)) as pass_sqft_mult,
                feas_curr as pass_financial_existing,
                
                CASE WHEN pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value THEN GREATEST(0, yield_curr - existing_units) ELSE 0 END as feasible_existing,
                CASE WHEN pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value THEN GREATEST(0, yield_pritzker - GREATEST(yield_curr, existing_units)) ELSE 0 END as new_pritzker,
                CASE WHEN pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value THEN GREATEST(0, yield_sb79 - GREATEST(yield_pritzker, existing_units)) ELSE 0 END as add_true_sb79,
                CASE WHEN pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value THEN GREATEST(0, yield_train - GREATEST(yield_pritzker, existing_units)) ELSE 0 END as add_train_only,
                CASE WHEN pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value THEN GREATEST(0, yield_hf - GREATEST(yield_pritzker, existing_units)) ELSE 0 END as add_train_and_hf_bus,
                CASE WHEN pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value THEN GREATEST(0, yield_combo - GREATEST(yield_pritzker, existing_units)) ELSE 0 END as add_train_and_bus_combo,
                
                CASE WHEN zone_class LIKE 'RM-%' OR zone_class LIKE 'RT-%' THEN parcels_combined ELSE 0 END as parcels_mf_zoned,
                CASE WHEN zone_class LIKE 'RM-%' OR zone_class LIKE 'RT-%' THEN area_sqft ELSE 0 END as area_mf_zoned,
                
                yield_curr, yield_pritzker, yield_sb79
                
            FROM final_yields
        )
    """

def run_spatial_pipeline(con, is_sandbox=False):
    config = load_config()
    eco = config.get('economic_assumptions', {})

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
            (COALESCE(v.bldg_value, 0.0) / CASE WHEN u.property_class LIKE '2%' OR u.property_class LIKE '3%' OR u.property_class LIKE '9%' THEN 0.10 ELSE 0.25 END) as tot_bldg_value,
            (COALESCE(v.land_value, 0.0) / CASE WHEN u.property_class LIKE '2%' OR u.property_class LIKE '3%' OR u.property_class LIKE '9%' THEN 0.10 ELSE 0.25 END) as tot_land_value
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
    print(f" ✅ ({time.time() - t0:.1f}s)")

    t0 = time.time()
    print("⏳ [5/5] Executing Real Estate Pro Forma equations...", end="", flush=True)

    def_condo = eco.get('default_condo_price_per_sqft', 350.0)
    cost_high = eco.get('const_cost_per_sqft_high', 300.0)
    cost_low = eco.get('const_cost_per_sqft_low', 240.0)
    acq_low = eco.get('default_acq_floor_per_sqft', 20.0)

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
            
            COALESCE(dcv.condo_price_per_sqft, {def_condo}) as condo_price_per_sqft,
            
            COALESCE(nsr.market_correction_multiplier, {DEFAULT_SALES_MULTIPLIER}) as market_correction_multiplier,
            
            CASE WHEN pd.neighborhood_name IN ('LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'LOOP', 'NEAR WEST SIDE') 
                 THEN {cost_high} ELSE {cost_low} END as const_cost_per_sqft,
                 
            COALESCE(dcv.acq_cost_floor_per_sqft, {acq_low}) as acq_cost_floor_per_sqft,

            pd.is_train_1320, pd.is_train_2640, pd.is_brt_1320, pd.is_brt_2640, pd.is_hf_1320, pd.all_bus_count, pd.hf_bus_count
        
        FROM step3_distances pd
        LEFT JOIN dynamic_condo_values dcv ON pd.neighborhood_name = dcv.neighborhood_name
        LEFT JOIN step4_sales_ratio nsr 
            ON pd.neighborhood_name = nsr.neighborhood_name 
            AND nsr.prop_category = CASE 
                WHEN pd.primary_prop_class IN ('211', '212', '213', '214') THEN 'MULTI_FAMILY'
                WHEN pd.primary_prop_class IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278') THEN 'SFH'
                WHEN pd.primary_prop_class LIKE '3%' OR pd.primary_prop_class LIKE '5%' THEN 'COMMERCIAL'
                ELSE 'OTHER' 
            END
    """)

    financial_ctes = get_financial_filter_ctes("step5_pro_forma_base", eco)
    con.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE step5_pro_forma AS 
        WITH {financial_ctes} 
        SELECT *,
               (feasible_existing + new_pritzker + add_true_sb79) as tot_true_sb79,
               (feasible_existing + new_pritzker + add_train_only) as tot_train_only,
               (feasible_existing + new_pritzker + add_train_and_hf_bus) as tot_train_and_hf_bus,
               (feasible_existing + new_pritzker + add_train_and_bus_combo) as tot_train_and_bus_combo
        FROM filtered_parcels
    """)
    print(f" ✅ ({time.time() - t0:.1f}s)")
