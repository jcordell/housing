import duckdb
import pandas as pd
import time
import yaml
from calculate_parcels import run_parcel_calculations

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_sandbox():
    global_start = time.time()
    config = load_config()
    db_file = config['database']['file_name']
    target_margin = config['economic_assumptions']['target_profit_margin']

    # Full HBU recalculation
    run_parcel_calculations(full_recalculate=True, is_sandbox=True)

    con = duckdb.connect(db_file)
    df_raw = con.execute("SELECT * FROM step5_pro_forma").df()

    # Pre-calculate individual ROIs for diagnostics
    df_raw['roi_curr'] = df_raw['value_per_new_unit'] / df_raw['cpu_current'].replace(0, float('inf'))
    df_raw['roi_pritzker'] = df_raw['value_per_new_unit'] / df_raw['cpu_pritzker'].replace(0, float('inf'))
    df_raw['roi_sb79'] = df_raw['value_per_new_unit'] / df_raw['cpu_sb79'].replace(0, float('inf'))

    print(f"\n✅ Total Sandbox Runtime: {time.time() - global_start:.2f} seconds\n")

    # -------------------------------------------------------------------------
    # SCENARIO 1: THE "ALMOST" PROFITABLE (Current Laws)
    # -------------------------------------------------------------------------
    print("\n" + "="*80)
    print(f"🔍 SCENARIO 1: NEAR-MISSES UNDER CURRENT ZONING (ROI 1.0 - {target_margin})")
    print("="*80)
    # Barely failed: ROI is positive but below target
    df_near = df_raw[(df_raw['roi_curr'] >= 1.0) & (df_raw['roi_curr'] < target_margin) & (df_raw['current_capacity'] > df_raw['existing_units'])]
    if df_near.empty:
        print("No 'Near-Miss' properties found.")
    else:
        for _, row in df_near.sort_values('roi_curr', ascending=False).head(3).iterrows():
            print_parcel_financials(row, 'CURRENT_NEAR_MISS', target_margin)

    # -------------------------------------------------------------------------
    # SCENARIO 2: PROFITABLE ONLY UNDER PRITZKER
    # -------------------------------------------------------------------------
    print("\n" + "="*80)
    print("🔍 SCENARIO 2: FLIPPED BY PRITZKER (Profitable only with 4-6 flat density)")
    print("="*80)
    df_p_only = df_raw[(df_raw['roi_curr'] < target_margin) & (df_raw['roi_pritzker'] >= target_margin)]
    if df_p_only.empty:
        print("No properties flipped exclusively by Pritzker baseline.")
    else:
        for _, row in df_p_only.head(3).iterrows():
            print_parcel_financials(row, 'PRITZKER_FLIP', target_margin)

    # -------------------------------------------------------------------------
    # SCENARIO 3: PROFITABLE ONLY UNDER SB79
    # -------------------------------------------------------------------------
    print("\n" + "="*80)
    print("🔍 SCENARIO 3: THE TRANSIT WHALES (Profitable only with SB79 density)")
    print("="*80)
    df_sb_only = df_raw[(df_raw['roi_pritzker'] < target_margin) & (df_raw['roi_sb79'] >= target_margin)]
    if df_sb_only.empty:
        print("No properties flipped exclusively by SB79 transit density.")
    else:
        for _, row in df_sb_only.head(3).iterrows():
            print_parcel_financials(row, 'SB79_FLIP', target_margin)

    # -------------------------------------------------------------------------
    # SCENARIO 4: THE IMPOSSIBLE PROJECTS
    # -------------------------------------------------------------------------
    print("\n" + "="*80)
    print("🔍 SCENARIO 4: BARELY QUALIFIED / NOT PROFITABLE AT ALL")
    print("="*80)
    # Focus on those closest to breakeven even under SB79
    df_fail = df_raw[(df_raw['roi_sb79'] > 0.8) & (df_raw['roi_sb79'] < 1.0)]
    if df_fail.empty:
        print("No properties in the 0.8 - 1.0 ROI range found.")
    else:
        for _, row in df_fail.sort_values('roi_sb79', ascending=False).head(3).iterrows():
            print_parcel_financials(row, 'TOTAL_FAIL', target_margin)

    con.close()

def print_parcel_financials(row, scenario, target_margin):
    addr = str(row['prop_address']).title() if pd.notnull(row['prop_address']) else 'Unknown'

    print(f"\n📍 {row['neighborhood_name']} | {addr} | Zone: {row['zone_class']}")
    print(f"   🏠 EXISTING: {row['existing_units']} units | Area: {row['area_sqft']:,.0f} sqft")

    if scenario == 'CURRENT_NEAR_MISS':
        print(f"   ❌ CURRENT ROI: {row['roi_curr']:.3f} (Needs {target_margin})")
        print(f"      To make this work: Market prices need to rise {((target_margin/row['roi_curr'])-1)*100:.1f}%")

    elif scenario == 'PRITZKER_FLIP':
        print(f"   📉 CURRENT ROI: {row['roi_curr']:.3f} (Fails)")
        print(f"   ✅ PRITZKER ROI: {row['roi_pritzker']:.3f} (Yields {row['yield_pritzker']} units)")

    elif scenario == 'SB79_FLIP':
        print(f"   📉 PRITZKER ROI: {row['roi_pritzker']:.3f} (Fails)")
        print(f"   🚆 SB79 ROI: {row['roi_sb79']:.3f} (Yields {row['yield_sb79']} units)")

    elif scenario == 'TOTAL_FAIL':
        print(f"   💀 TOTAL FAIL: Even with SB79, ROI is only {row['roi_sb79']:.3f}")
        print(f"      Reason: Acquisition cost (${row['acquisition_cost']:,.0f}) is too high for the revenue.")

    print(f"   🏗️  PPSF: ${row['condo_price_per_sqft']:,.2f}/sqft | Land Cost: ${row['acquisition_cost']:,.0f}")
    print("-" * 60)

if __name__ == "__main__":
    run_sandbox()
