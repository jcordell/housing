CREATE OR REPLACE TABLE step5_pro_forma AS
WITH combined AS (
    SELECT
        up.center_geom, up.neighborhood_name, up.area_sqft, up.zone_class, up.parcels_combined,
        up.is_train_1320, up.is_train_2640, up.is_brt_1320, up.is_brt_2640, up.is_hf_1320, up.all_bus_count, up.hf_bus_count,
        COALESCE(up.existing_units, 0.0) as existing_units,
        COALESCE(up.primary_prop_class, 'UNKNOWN') as primary_prop_class,
        COALESCE(up.tot_bldg_value, 0.0) as tot_bldg_value,
        COALESCE(up.tot_land_value, 0.0) as tot_land_value,
        COALESCE(up.building_age, 0) as building_age,
        COALESCE(up.existing_sqft, 0.0) as existing_sqft,
        up.prop_address,
        up.market_correction_multiplier,
        COALESCE(dcv.condo_price_per_sqft, {{ default_condo_price_per_sqft }}) as condo_price_per_sqft,
        CASE WHEN up.neighborhood_name IN ('LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'LOOP', 'NEAR WEST SIDE')
             THEN {{ const_cost_per_sqft_high }} ELSE {{ const_cost_per_sqft_low }} END as const_cost_per_sqft,
        COALESCE(dcv.acq_cost_floor_per_sqft, {{ default_acq_floor_per_sqft }}) as acq_cost_floor_per_sqft,
        {{ target_profit_margin }} as target_profit_margin,
        {{ min_unit_size_sqft }} as min_unit_size_sqft
    FROM unified_properties up
    LEFT JOIN dynamic_condo_values dcv ON up.neighborhood_name = dcv.neighborhood_name
),
raw_capacities AS (
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
    FROM combined
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
            AND primary_prop_class NOT IN ('UNKNOWN', 'EX', '0', '1', '4', '93')
            AND primary_prop_class != '299'
            AND primary_prop_class NOT LIKE '299%'
            AND primary_prop_class NOT LIKE '599%'
            AND primary_prop_class NOT LIKE '8%'
            AND primary_prop_class NOT LIKE '0%'
            AND primary_prop_class NOT LIKE '1%'
            AND prop_address NOT ILIKE '%CHURCH%'
            AND prop_address NOT ILIKE '%RELIGIOUS%'
        ) as pass_prop_class,
        ((tot_bldg_value + tot_land_value) >= 1000) as pass_min_value,
        ((existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560) as pass_lot_density
    FROM raw_capacities
),
financial_metrics AS (
    SELECT *,
        (area_sqft * {{ far_current }}) as gsf_curr,
        (area_sqft * {{ far_pritzker }}) as gsf_pritzker,
        (area_sqft * CASE WHEN cap_sb79_raw > 0 THEN {{ far_sb79 }} ELSE {{ far_pritzker }} END) as gsf_sb79,
        (area_sqft * CASE WHEN cap_train_raw > 0 THEN {{ far_train }} ELSE {{ far_pritzker }} END) as gsf_train,
        (area_sqft * CASE WHEN cap_hf_raw > 0 THEN {{ far_hf }} ELSE {{ far_pritzker }} END) as gsf_hf,
        (area_sqft * CASE WHEN cap_combo_raw > 0 THEN {{ far_combo }} ELSE {{ far_pritzker }} END) as gsf_combo,

        ((area_sqft * {{ far_current }}) * CASE WHEN cap_curr <= 2 THEN 0.90
                 WHEN cap_curr <= 4 THEN 0.75
                 WHEN cap_curr <= 9 THEN 0.78
                 WHEN cap_curr <= 19 THEN 0.80
                 ELSE 0.82 END
        ) as nra_curr,

        ((area_sqft * {{ far_pritzker }}) * CASE WHEN cap_pritzker <= 2 THEN 0.90
                 WHEN cap_pritzker <= 6 THEN 0.87
                 WHEN cap_pritzker <= 15 THEN 0.85
                 ELSE 0.82 END
        ) as nra_pritzker,

        ((area_sqft * CASE WHEN cap_sb79_raw > 0 THEN {{ far_sb79 }} ELSE {{ far_pritzker }} END) * CASE WHEN cap_sb79 <= 2 THEN 0.90
                 WHEN cap_sb79 <= 6 THEN 0.87
                 WHEN cap_sb79 <= 15 THEN 0.85
                 ELSE 0.82 END
        ) as nra_sb79,

        ((area_sqft * CASE WHEN cap_train_raw > 0 THEN {{ far_train }} ELSE {{ far_pritzker }} END) * CASE WHEN cap_train_only <= 2 THEN 0.90
                 WHEN cap_train_only <= 6 THEN 0.87
                 WHEN cap_train_only <= 15 THEN 0.85
                 ELSE 0.82 END
        ) as nra_train,

        ((area_sqft * CASE WHEN cap_hf_raw > 0 THEN {{ far_hf }} ELSE {{ far_pritzker }} END) * CASE WHEN cap_train_hf <= 2 THEN 0.90
                 WHEN cap_train_hf <= 6 THEN 0.87
                 WHEN cap_train_hf <= 15 THEN 0.85
                 ELSE 0.82 END
        ) as nra_hf,

        ((area_sqft * CASE WHEN cap_combo_raw > 0 THEN {{ far_combo }} ELSE {{ far_pritzker }} END) * CASE WHEN cap_train_combo <= 2 THEN 0.90
                 WHEN cap_train_combo <= 6 THEN 0.87
                 WHEN cap_train_combo <= 15 THEN 0.85
                 ELSE 0.82 END
        ) as nra_combo
    FROM capacities
),
unit_capacity AS (
    SELECT *,
        LEAST(cap_curr, FLOOR(nra_curr / min_unit_size_sqft)) as final_cap_curr,
        LEAST(cap_pritzker, FLOOR(nra_pritzker / min_unit_size_sqft)) as final_cap_pritzker,
        LEAST(cap_sb79, FLOOR(nra_sb79 / min_unit_size_sqft)) as final_cap_sb79,
        LEAST(cap_train_only, FLOOR(nra_train / min_unit_size_sqft)) as final_cap_train,
        LEAST(cap_train_hf, FLOOR(nra_hf / min_unit_size_sqft)) as final_cap_hf,
        LEAST(cap_train_combo, FLOOR(nra_combo / min_unit_size_sqft)) as final_cap_combo
    FROM financial_metrics
),
revenue_metrics AS (
    SELECT *,
        (nra_curr * condo_price_per_sqft) * CASE WHEN final_cap_curr > 10 THEN 0.90 ELSE 1.0 END as rev_curr,
        (nra_pritzker * condo_price_per_sqft) * CASE WHEN final_cap_pritzker > 10 THEN 0.90 ELSE 1.0 END as rev_pritzker,
        (nra_sb79 * condo_price_per_sqft) * CASE WHEN final_cap_sb79 > 10 THEN 0.90 ELSE 1.0 END as rev_sb79,
        (nra_train * condo_price_per_sqft) * CASE WHEN final_cap_train > 10 THEN 0.90 ELSE 1.0 END as rev_train,
        (nra_hf * condo_price_per_sqft) * CASE WHEN final_cap_hf > 10 THEN 0.90 ELSE 1.0 END as rev_hf,
        (nra_combo * condo_price_per_sqft) * CASE WHEN final_cap_combo > 10 THEN 0.90 ELSE 1.0 END as rev_combo
    FROM unit_capacity
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
        (rev_curr > (cost_curr * target_profit_margin)) as feas_curr,
        (rev_pritzker > (cost_pritzker * target_profit_margin)) as feas_pritzker,
        (rev_sb79 > (cost_sb79 * target_profit_margin)) as feas_sb79,
        (rev_train > (cost_train * target_profit_margin)) as feas_train,
        (rev_hf > (cost_hf * target_profit_margin)) as feas_hf,
        (rev_combo > (cost_combo * target_profit_margin)) as feas_combo
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
        final_cap_curr as current_capacity, final_cap_pritzker as pritzker_capacity, final_cap_sb79 as cap_true_sb79,
        primary_prop_class, tot_bldg_value, tot_land_value, market_correction_multiplier,
        cpu_current, cpu_pritzker, cpu_sb79,
        rev_curr, rev_pritzker, rev_sb79,
        cost_curr, cost_pritzker, cost_sb79,
        pass_max_units, pass_age_value, pass_zoning_class, pass_prop_class, pass_min_value, pass_lot_density,
        (yield_curr >= (GREATEST(existing_units, 1.0) * 2.0)) as pass_unit_mult,
        (final_cap_curr >= (GREATEST(existing_units, 1.0) * 2.0)) as pass_unit_mult_raw,
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
SELECT *,
       (feasible_existing + new_pritzker + add_true_sb79) as tot_true_sb79,
       (feasible_existing + new_pritzker + add_train_only) as tot_train_only,
       (feasible_existing + new_pritzker + add_train_and_hf_bus) as tot_train_and_hf_bus,
       (feasible_existing + new_pritzker + add_train_and_bus_combo) as tot_train_and_bus_combo
FROM filtered_parcels;
