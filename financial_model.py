# Rents
CHICAGO_NEW_BUILD_RENTS = {
    'LINCOLN PARK': 2800.0, 'NEAR NORTH SIDE': 2900.0, 'LOOP': 2900.0, 'NEAR WEST SIDE': 2800.0,
    'LAKE VIEW': 2400.0, 'WEST TOWN': 2500.0, 'LOGAN SQUARE': 2300.0, 'NORTH CENTER': 2300.0,
    'LINCOLN SQUARE': 2100.0, 'UPTOWN': 2000.0, 'EDGEWATER': 1900.0, 'AVONDALE': 1900.0,
    'HYDE PARK': 2100.0, 'BRIDGEPORT': 1800.0, 'PORTAGE PARK': 1700.0, 'ASHBURN': 1500.0,
    'AUSTIN': 1400.0, 'ENGLEWOOD': 1300.0, 'WASHINGTON PARK': 1500.0
}
DEFAULT_NEW_BUILD_RENT = 1600.0

# Sales Ratios
CHICAGO_SALES_MULTIPLIERS = {
    'LINCOLN PARK': 1.65, 'LAKE VIEW': 1.55, 'NEAR NORTH SIDE': 1.60,
    'WEST TOWN': 1.55, 'LOGAN SQUARE': 1.50, 'AUSTIN': 1.25, 'ASHBURN': 1.20,
}
DEFAULT_SALES_MULTIPLIER = 1.40

# 🔥 NEW: Fallback Construction Costs (Cost per Unit)
CHICAGO_CONSTRUCTION_COSTS = {
    'LINCOLN PARK': 350000.0, 'NEAR NORTH SIDE': 380000.0, 'LOOP': 400000.0,
    'LAKE VIEW': 320000.0, 'WEST TOWN': 320000.0, 'LOGAN SQUARE': 300000.0,
    'AUSTIN': 240000.0, 'ASHBURN': 240000.0, 'ENGLEWOOD': 230000.0
}
DEFAULT_CONSTRUCTION_COST = 275000.0

def get_financial_filter_ctes(source_table_name):
    return f"""
        pro_forma_parcels AS (
            SELECT center_geom, area_sqft, parcels_combined, zone_class, neighborhood_name, prop_address,
                
                -- ===========================================================
                -- 💡 FINANCIAL ASSUMPTIONS 
                -- ===========================================================
                
                local_rent,
                ((local_rent * 12.0) / 0.055) as value_per_new_unit,
                
                market_correction_multiplier,
                GREATEST((COALESCE(tot_bldg_value, 0.0) + COALESCE(tot_land_value, 0.0)) * market_correction_multiplier, 10000.0) as acquisition_cost,
                
                -- Dynamic Construction Costs (Pulled from Building Permits)
                COALESCE(dynamic_cost_per_unit, {DEFAULT_CONSTRUCTION_COST}) as cost_per_unit_low_density,
                
                -- Mid-rise/high-density costs roughly 30% more per unit due to elevators/steel/union labor
                COALESCE(dynamic_cost_per_unit * 1.30, {DEFAULT_CONSTRUCTION_COST} * 1.30) as cost_per_unit_high_density,
                
                1.15 as target_profit_margin,
                
                -- ===========================================================
                
                COALESCE(tot_existing_units, 0.0) as existing_units, 
                COALESCE(building_age, 0) as building_age,
                COALESCE(existing_sqft, 0.0) as existing_sqft,
                
                LEAST(current_capacity, 200) as current_capacity, 
                LEAST(pritzker_capacity, 200) as pritzker_capacity, 
                LEAST(cap_true_sb79, 200) as cap_true_sb79, 
                LEAST(cap_train_only, 200) as cap_train_only, 
                LEAST(cap_train_and_hf_bus, 200) as cap_train_and_hf_bus, 
                LEAST(cap_train_and_bus_combo, 200) as cap_train_and_bus_combo,
                
                primary_prop_class, tot_bldg_value, tot_land_value
                
            FROM {source_table_name}
        ),
        
        filtered_parcels AS (
            SELECT center_geom, area_sqft, parcels_combined, zone_class, neighborhood_name, prop_address,
                local_rent, value_per_new_unit, acquisition_cost, existing_units, building_age, existing_sqft,
                current_capacity, primary_prop_class, tot_bldg_value, tot_land_value,
                cost_per_unit_low_density, cost_per_unit_high_density, target_profit_margin, market_correction_multiplier,
                
                CASE WHEN 
                    current_capacity >= (GREATEST(existing_units, 1.0) * 2.0) AND           
                    (current_capacity * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND  
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND                    
                    existing_units < 40 AND                                                 
                    (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND                           
                    zone_class NOT IN ('OS', 'POS', 'PMD') AND                              
                    primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND                                
                    primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND                                   
                    (current_capacity * value_per_new_unit) > (acquisition_cost + (current_capacity * cost_per_unit_low_density)) * target_profit_margin
                THEN GREATEST(0, current_capacity - existing_units) ELSE 0 END as feasible_existing,

                CASE WHEN 
                    pritzker_capacity >= (GREATEST(existing_units, 1.0) * 2.0) AND 
                    (pritzker_capacity * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND 
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (pritzker_capacity * value_per_new_unit) > (acquisition_cost + (pritzker_capacity * cost_per_unit_low_density)) * target_profit_margin
                THEN GREATEST(0, pritzker_capacity - current_capacity) ELSE 0 END as new_pritzker,
                
                CASE WHEN 
                    cap_true_sb79 >= (GREATEST(existing_units, 1.0) * 2.0) AND 
                    (cap_true_sb79 * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND 
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_true_sb79,
                
                CASE WHEN 
                    cap_true_sb79 >= (GREATEST(existing_units, 1.0) * 2.0) AND 
                    (cap_true_sb79 * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND 
                    (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                    (cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_true_sb79) - current_capacity) ELSE 0 END as tot_true_sb79,
                
                CASE WHEN cap_train_only >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_only * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_only,
                
                CASE WHEN cap_train_only >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_only * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_only) - current_capacity) ELSE 0 END as tot_train_only,
                
                CASE WHEN cap_train_and_hf_bus >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_hf_bus * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_hf_bus,
                
                CASE WHEN cap_train_and_hf_bus >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_hf_bus * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_hf_bus) - current_capacity) ELSE 0 END as tot_train_and_hf_bus,
                
                CASE WHEN cap_train_and_bus_combo >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_bus_combo * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_bus_combo,
                
                CASE WHEN cap_train_and_bus_combo >= (GREATEST(existing_units, 1.0) * 2.0) AND (cap_train_and_bus_combo * 800.0) >= (GREATEST(existing_sqft, 1.0) * 1.25) AND (existing_sqft / GREATEST(area_sqft, 1.0)) < 1.5 AND existing_units < 40 AND (building_age >= 35 OR (building_age = 0 AND tot_bldg_value < 100000)) AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '599%' AND primary_prop_class NOT LIKE '8%' AND primary_prop_class != 'EX' AND
                (cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cost_per_unit_high_density)) * target_profit_margin
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_bus_combo) - current_capacity) ELSE 0 END as tot_train_and_bus_combo,

                parcels_combined, area_sqft,
                CASE WHEN zone_class NOT LIKE 'RS-1%' AND zone_class NOT LIKE 'RS-2%' AND zone_class NOT LIKE 'RS-3%' THEN parcels_combined ELSE 0 END as parcels_mf_zoned,
                CASE WHEN zone_class NOT LIKE 'RS-1%' AND zone_class NOT LIKE 'RS-2%' AND zone_class NOT LIKE 'RS-3%' THEN area_sqft ELSE 0 END as area_mf_zoned
            
            FROM pro_forma_parcels
        )
    """
