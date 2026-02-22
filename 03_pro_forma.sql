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

        {{ default_rent_per_sqft }} as rent_per_sqft,
        {{ default_cap_rate }} as cap_rate,
        {{ cost_2_4_units }} as cost_2_4_units,
        {{ cost_5_15_units }} as cost_5_15_units,
        {{ cost_15_plus_units }} as cost_15_plus_units,

        CASE WHEN up.neighborhood_name IN ('LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'LOOP', 'NEAR WEST SIDE')
             THEN {{ high_cost_acq_floor_per_sqft }} ELSE {{ default_acq_floor_per_sqft }} END as acq_cost_floor_per_sqft,

        {{ target_profit_margin }} as target_profit_margin,
        {{ unit_size_sqft }} as unit_size_sqft

    FROM unified_properties up
),
capacities AS (
    SELECT *,
        (rent_per_sqft * unit_size_sqft * 12.0) / cap_rate as value_per_new_unit,
        GREATEST(
            (tot_bldg_value + tot_land_value) * market_correction_multiplier,
            area_sqft * acq_cost_floor_per_sqft
        ) as acquisition_cost,

        LEAST(150, GREATEST(1, CASE
            WHEN zone_class LIKE 'RS-1%' OR zone_class LIKE 'RS-2%' THEN FLOOR(area_sqft / 5000)
            WHEN zone_class LIKE 'RS-3%' THEN FLOOR(area_sqft / 2500)
            WHEN zone_class LIKE 'RT-3.5%' THEN FLOOR(area_sqft / 1250)
            WHEN zone_class LIKE 'RT-4%' THEN FLOOR(area_sqft / 1000)
            WHEN zone_class LIKE 'RM-4.5%' OR zone_class LIKE 'RM-5%' THEN FLOOR(area_sqft / 400)
            WHEN zone_class LIKE 'RM-6%' OR zone_class LIKE 'RM-6.5%' THEN FLOOR(area_sqft / 200)
            WHEN zone_class LIKE '%-1' THEN FLOOR(area_sqft / 1000)
            WHEN zone_class LIKE '%-2' OR zone_class LIKE '%-3' THEN FLOOR(area_sqft / 400)
            WHEN zone_class LIKE '%-5' OR zone_class LIKE '%-6' THEN FLOOR(area_sqft / 200)
            ELSE FLOOR(area_sqft / 1000) END)) as current_capacity,

        LEAST(150, CASE
            WHEN zone_class SIMILAR TO '(RS|RT|RM).*' THEN
                CASE WHEN area_sqft < 2500 THEN 1 WHEN area_sqft < 5000 THEN 4 WHEN area_sqft < 7500 THEN 6 ELSE 8 END
            ELSE 0 END) as pritzker_capacity,

        LEAST(150, CASE
            WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120)
            WHEN is_train_2640 OR is_brt_1320 OR hf_bus_count >= 2 THEN FLOOR((area_sqft / 43560.0) * 100)
            WHEN is_brt_2640 THEN FLOOR((area_sqft / 43560.0) * 80)
            ELSE 0 END) as cap_true_sb79,

        LEAST(150, CASE
            WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120)
            WHEN is_train_2640 THEN FLOOR((area_sqft / 43560.0) * 100)
            ELSE 0 END) as cap_train_only,

        LEAST(150, CASE
            WHEN is_train_2640 AND is_hf_1320 THEN CASE WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END
            ELSE 0 END) as cap_train_and_hf_bus,

        LEAST(150, CASE
            WHEN is_train_2640 AND (is_hf_1320 OR all_bus_count >= 2) THEN CASE WHEN is_train_1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END
            ELSE 0 END) as cap_train_and_bus_combo
    FROM combined
),
cost_applied AS (
    SELECT *,
        CASE WHEN current_capacity BETWEEN 5 AND 14 THEN cost_5_15_units WHEN current_capacity >= 15 THEN cost_15_plus_units ELSE cost_2_4_units END as cpu_current,
        CASE WHEN pritzker_capacity BETWEEN 5 AND 14 THEN cost_5_15_units WHEN pritzker_capacity >= 15 THEN cost_15_plus_units ELSE cost_2_4_units END as cpu_pritzker,
        CASE WHEN cap_true_sb79 BETWEEN 5 AND 14 THEN cost_5_15_units WHEN cap_true_sb79 >= 15 THEN cost_15_plus_units ELSE cost_2_4_units END as cpu_sb79,
        CASE WHEN cap_train_only BETWEEN 5 AND 14 THEN cost_5_15_units WHEN cap_train_only >= 15 THEN cost_15_plus_units ELSE cost_2_4_units END as cpu_train,
        CASE WHEN cap_train_and_hf_bus BETWEEN 5 AND 14 THEN cost_5_15_units WHEN cap_train_and_hf_bus >= 15 THEN cost_15_plus_units ELSE cost_2_4_units END as cpu_hf_bus,
        CASE WHEN cap_train_and_bus_combo BETWEEN 5 AND 14 THEN cost_5_15_units WHEN cap_train_and_bus_combo >= 15 THEN cost_15_plus_units ELSE cost_2_4_units END as cpu_combo
    FROM capacities
),
filtered AS (
    SELECT *,
        (current_capacity >= (GREATEST(existing_units, 1.0) * 2.0)) as pass_unit_mult,
        ((current_capacity * unit_size_sqft) >= (GREATEST(existing_sqft, 1.0) * 1.25)) as pass_sqft_mult,
        ((existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND area_sqft <= 43560) as pass_lot_density,
        (existing_units < 40) as pass_max_units,
        (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 250000)) as pass_age_value,
        (zone_class NOT IN ('OS', 'POS', 'PMD')) as pass_zoning_class,
        (primary_prop_class IS NOT NULL AND primary_prop_class != 'UNKNOWN' AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '3%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX') as pass_prop_class,
        ((tot_bldg_value + tot_land_value) >= 1000) as pass_min_value,
        ((current_capacity * value_per_new_unit) > (acquisition_cost + (current_capacity * cpu_current)) * target_profit_margin) as pass_financial_existing
    FROM cost_applied
),
deltas AS (
    SELECT *,
        CASE WHEN pass_unit_mult AND pass_sqft_mult AND pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_financial_existing
             THEN GREATEST(0, current_capacity - existing_units) ELSE 0 END as feasible_existing,

        CASE WHEN pritzker_capacity >= (GREATEST(existing_units, 1.0) * 2.0) AND (pritzker_capacity * unit_size_sqft) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value AND
             ((pritzker_capacity * value_per_new_unit) > (acquisition_cost + (pritzker_capacity * cpu_pritzker)) * target_profit_margin)
             THEN GREATEST(0, pritzker_capacity - current_capacity) ELSE 0 END as new_pritzker,

        CASE WHEN cap_true_sb79 >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_true_sb79 * unit_size_sqft) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value AND
             ((cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cpu_sb79)) * target_profit_margin)
             THEN GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_true_sb79,

        CASE WHEN cap_train_only >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_only * unit_size_sqft) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value AND
             ((cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cpu_train)) * target_profit_margin)
             THEN GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_only,

        CASE WHEN cap_train_and_hf_bus >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_hf_bus * unit_size_sqft) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value AND
             ((cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cpu_hf_bus)) * target_profit_margin)
             THEN GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_hf_bus,

        CASE WHEN cap_train_and_bus_combo >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_bus_combo * unit_size_sqft) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND pass_lot_density AND pass_max_units AND pass_age_value AND pass_zoning_class AND pass_prop_class AND pass_min_value AND
             ((cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cpu_combo)) * target_profit_margin)
             THEN GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_bus_combo,

        CASE WHEN zone_class LIKE 'RM-%' OR zone_class LIKE 'RT-%' THEN parcels_combined ELSE 0 END as parcels_mf_zoned,
        CASE WHEN zone_class LIKE 'RM-%' OR zone_class LIKE 'RT-%' THEN area_sqft ELSE 0 END as area_mf_zoned
    FROM filtered
)
SELECT *,
       (new_pritzker + add_true_sb79) as tot_true_sb79,
       (new_pritzker + add_train_only) as tot_train_only,
       (new_pritzker + add_train_and_hf_bus) as tot_train_and_hf_bus,
       (new_pritzker + add_train_and_bus_combo) as tot_train_and_bus_combo
FROM deltas;
