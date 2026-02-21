def get_financial_filter_ctes(source_table_name):
    """
    Returns the Common Table Expressions (CTEs) for the Real Estate Pro Forma model.
    By passing in a source_table_name, this logic can be injected into any DuckDB query.
    """
    return f"""
        pro_forma_parcels AS (
            SELECT center_geom, area_sqft, parcels_combined, zone_class, neighborhood_name,
                
                -- ===========================================================
                -- 💡 TWEAK YOUR FINANCIAL ASSUMPTIONS HERE
                -- ===========================================================
                
                -- Value of a new unit = (Monthly Rent * 12) / 5.5% Cap Rate
                ((local_rent * 12.0) / 0.055) as value_per_new_unit,
                
                -- Acquisition Cost = Land + Building (min $10k to prevent div by zero)
                GREATEST((COALESCE(tot_bldg_value, 0.0) + COALESCE(tot_land_value, 0.0)), 10000.0) as acquisition_cost,
                
                -- Construction Costs (800 sq ft unit + 20% soft costs)
                -- Missing Middle (Pritzker): $250/sqft hard costs -> $240,000 per unit
                240000.0 as cost_per_unit_low_density,
                -- Mid-Rise (SB 79): $350/sqft hard costs -> $336,000 per unit
                336000.0 as cost_per_unit_high_density,
                
                -- Target Profit Margin (1.15 = 15% yield on cost)
                1.15 as target_profit_margin,
                
                -- ===========================================================
                
                COALESCE(tot_existing_units, 0.0) as existing_units, 
                current_capacity, pritzker_capacity, cap_true_sb79, cap_train_only, cap_train_and_hf_bus, cap_train_and_bus_combo,
                primary_prop_class
                
            FROM {source_table_name}
        ),
        
        filtered_parcels AS (
            SELECT center_geom, area_sqft, parcels_combined, zone_class, neighborhood_name,
                
                -- Status Quo (Baseline Feasible under current zoning)
                CASE WHEN 
                    current_capacity > existing_units AND
                    (current_capacity * value_per_new_unit) > (acquisition_cost + (current_capacity * cost_per_unit_low_density)) * target_profit_margin AND
                    existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
                THEN GREATEST(0, current_capacity - existing_units) ELSE 0 END as feasible_existing,

                -- Pritzker
                CASE WHEN 
                    pritzker_capacity > existing_units AND
                    (pritzker_capacity * value_per_new_unit) > (acquisition_cost + (pritzker_capacity * cost_per_unit_low_density)) * target_profit_margin AND
                    existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
                THEN GREATEST(0, pritzker_capacity - current_capacity) ELSE 0 END as new_pritzker,
                
                -- True SB 79 (Uses the higher high_density construction cost)
                CASE WHEN 
                    cap_true_sb79 > existing_units AND
                    (cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cost_per_unit_high_density)) * target_profit_margin AND
                    existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
                THEN GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_true_sb79,
                
                CASE WHEN 
                    cap_true_sb79 > existing_units AND
                    (cap_true_sb79 * value_per_new_unit) > (acquisition_cost + (cap_true_sb79 * cost_per_unit_high_density)) * target_profit_margin AND
                    existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%'
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_true_sb79) - current_capacity) ELSE 0 END as tot_true_sb79,
                
                -- Train Only
                CASE WHEN cap_train_only > existing_units AND (cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cost_per_unit_high_density)) * target_profit_margin AND existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%' 
                THEN GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_only,
                CASE WHEN cap_train_only > existing_units AND (cap_train_only * value_per_new_unit) > (acquisition_cost + (cap_train_only * cost_per_unit_high_density)) * target_profit_margin AND existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%' 
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_only) - current_capacity) ELSE 0 END as tot_train_only,
                
                -- Train + HF Bus
                CASE WHEN cap_train_and_hf_bus > existing_units AND (cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cost_per_unit_high_density)) * target_profit_margin AND existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%' 
                THEN GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_hf_bus,
                CASE WHEN cap_train_and_hf_bus > existing_units AND (cap_train_and_hf_bus * value_per_new_unit) > (acquisition_cost + (cap_train_and_hf_bus * cost_per_unit_high_density)) * target_profit_margin AND existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%' 
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_hf_bus) - current_capacity) ELSE 0 END as tot_train_and_hf_bus,
                
                -- Train + Bus Combo
                CASE WHEN cap_train_and_bus_combo > existing_units AND (cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cost_per_unit_high_density)) * target_profit_margin AND existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%' 
                THEN GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_bus_combo,
                CASE WHEN cap_train_and_bus_combo > existing_units AND (cap_train_and_bus_combo * value_per_new_unit) > (acquisition_cost + (cap_train_and_bus_combo * cost_per_unit_high_density)) * target_profit_margin AND existing_units < 20.0 AND zone_class NOT IN ('OS', 'POS', 'PMD') AND primary_prop_class NOT LIKE '299%' AND primary_prop_class NOT LIKE '8%' 
                THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_bus_combo) - current_capacity) ELSE 0 END as tot_train_and_bus_combo,

                -- Base zoning data
                parcels_combined, area_sqft,
                CASE WHEN zone_class NOT LIKE 'RS-1%' AND zone_class NOT LIKE 'RS-2%' AND zone_class NOT LIKE 'RS-3%' THEN parcels_combined ELSE 0 END as parcels_mf_zoned,
                CASE WHEN zone_class NOT LIKE 'RS-1%' AND zone_class NOT LIKE 'RS-2%' AND zone_class NOT LIKE 'RS-3%' THEN area_sqft ELSE 0 END as area_mf_zoned
            
            FROM pro_forma_parcels
        )
    """
