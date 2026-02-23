import duckdb
import pandas as pd
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_lp_comparison():
    config = load_config()
    db_file = config['database']['file_name']
    con = duckdb.connect(db_file)

    print("\n" + "="*120)
    print("LINCOLN PARK: TOP 50 NEAR-MISSES (SB79 TRANSIT UPZONING)")
    print("="*120)

    # Note: raw_roi is (Total Revenue / Total Cost). 1.15 is the goal.
    sb79_query = """
    SELECT
        prop_address,
        zone_class as zone,
        current_capacity as base_u,
        cap_true_sb79 as prop_u,
        acquisition_cost as acq,
        (cap_true_sb79 * cpu_sb79) as const_cost,
        (cap_true_sb79 * value_per_new_unit) as revenue,
        ((cap_true_sb79 * value_per_new_unit) / NULLIF(acquisition_cost + (cap_true_sb79 * cpu_sb79), 0)) as raw_roi
    FROM step5_pro_forma
    WHERE neighborhood_name = 'LINCOLN PARK'
      AND cap_true_sb79 > current_capacity
      AND raw_roi < 1.15
    ORDER BY raw_roi DESC
    LIMIT 50
    """

    df_sb79 = con.execute(sb79_query).df()
    print_table(df_sb79)

    print("\n" + "="*120)
    print("LINCOLN PARK: TOP 50 NEAR-MISSES (PRITZKER BASELINE UPZONING)")
    print("="*120)

    pritzker_query = """
    SELECT
        prop_address,
        zone_class as zone,
        current_capacity as base_u,
        pritzker_capacity as prop_u,
        acquisition_cost as acq,
        (pritzker_capacity * cpu_pritzker) as const_cost,
        (pritzker_capacity * value_per_new_unit) as revenue,
        ((pritzker_capacity * value_per_new_unit) / NULLIF(acquisition_cost + (pritzker_capacity * cpu_pritzker), 0)) as raw_roi
    FROM step5_pro_forma
    WHERE neighborhood_name = 'LINCOLN PARK'
      AND pritzker_capacity > current_capacity
      AND raw_roi < 1.15
    ORDER BY raw_roi DESC
    LIMIT 50
    """

    df_pritzker = con.execute(pritzker_query).df()
    print_table(df_pritzker)
    con.close()

def print_table(df):
    if df.empty:
        print("No properties found in this scenario.")
        return

    # Format dollars for readability
    cols_to_fix = ['acq', 'const_cost', 'revenue']
    for col in cols_to_fix:
        df[col] = df[col].apply(lambda x: f"${x/1000:,.0f}k")

    df['raw_roi'] = df['raw_roi'].round(3)
    print(df.to_string(index=False))

if __name__ == "__main__":
    run_lp_comparison()
