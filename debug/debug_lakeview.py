import duckdb
import pandas as pd
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_lakeview_debug():
    config = load_config()
    db_file = config['database']['file_name']
    con = duckdb.connect(db_file)
    target_margin = config['economic_assumptions']['target_profit_margin']

    print("\n" + "="*80)
    print("1. LAKEVIEW FUNNEL: Where are lots dying?")
    print("="*80)

    funnel_query = f"""
        SELECT
            COUNT(*) as total_parcels,
            SUM(CAST(pass_zoning_class AND pass_prop_class AS INT)) as legally_residential,
            SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density AS INT)) as physically_viable,
            SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density AND pass_unit_mult_raw AS INT)) as capacity_viable,
            SUM(CAST(pass_financial_existing AS INT)) as profitable_status_quo,
            SUM(CAST((rev_pritzker > cost_pritzker * {target_margin}) AS INT)) as profitable_pritzker,
            SUM(CAST((rev_sb79 > cost_sb79 * {target_margin}) AS INT)) as profitable_sb79
        FROM step5_pro_forma
        WHERE neighborhood_name = 'LAKE VIEW'
    """
    df_funnel = con.execute(funnel_query).df()
    print(df_funnel.to_string(index=False))

    print("\n" + "="*80)
    print("2. UNIT ECONOMICS: The Per-Square-Foot Reality (Averages)")
    print("="*80)

    econ_query = """
        SELECT
            COUNT(*) as lot_count,
            ROUND(AVG(area_sqft), 0) as avg_lot_sqft,
            ROUND(AVG(acquisition_cost), 0) as avg_acq_cost,
            ROUND(AVG(acquisition_cost / area_sqft), 2) as acq_cost_per_lot_sqft,
            ROUND(AVG(condo_price_per_sqft), 2) as expected_condo_sell_ppsf,
            ROUND(AVG(market_correction_multiplier), 2) as avg_assessor_multiplier
        FROM step5_pro_forma
        WHERE neighborhood_name = 'LAKE VIEW'
          AND pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density
    """
    df_econ = con.execute(econ_query).df()
    print(df_econ.to_string(index=False))

    print("\n" + "="*80)
    print("3. THE NEAR-MISSES: Top 5 Closest to Breakeven under SB79")
    print("="*80)

    near_miss_query = """
        SELECT
            prop_address,
            zone_class,
            area_sqft,
            existing_units,
            cap_true_sb79 as allowed_units,
            ROUND(acquisition_cost, 0) as acq_cost,
            ROUND(cost_sb79 - acquisition_cost, 0) as construction_cost,
            ROUND(cost_sb79, 0) as total_cost,
            ROUND(rev_sb79, 0) as total_revenue,
            ROUND(rev_sb79 / cost_sb79, 3) as raw_roi
        FROM step5_pro_forma
        WHERE neighborhood_name = 'LAKE VIEW'
          AND pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density
        ORDER BY raw_roi DESC
        LIMIT 5
    """
    df_near_miss = con.execute(near_miss_query).df()

    format_cols = ['acq_cost', 'construction_cost', 'total_cost', 'total_revenue']
    for col in format_cols:
        df_near_miss[col] = df_near_miss[col].apply(lambda x: f"${x:,.0f}")

    print(df_near_miss.to_string(index=False))

    print("\n" + "="*80)
    print("4. THE BREAKEVEN GAP: What needs to change for the best lot?")
    print("="*80)

    if not df_near_miss.empty:
        best_lot = df_near_miss.iloc[0]
        current_roi = best_lot['raw_roi']

        print(f"Address: {best_lot['prop_address']}")
        print(f"Current ROI: {current_roi:.3f} (Target: {target_margin})")

        if current_roi > 0:
            price_increase_needed = ((target_margin / current_roi) - 1) * 100
            print(f"-> Condo sell prices need to increase by {price_increase_needed:.1f}% to make this profitable.")
        else:
            print("-> ROI is zero or negative. Math is fundamentally broken for this lot.")

    con.close()

if __name__ == "__main__":
    run_lakeview_debug()
