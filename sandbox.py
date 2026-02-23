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

    run_parcel_calculations(full_recalculate=True, is_sandbox=True)

    con = duckdb.connect(db_file)
    df_raw = con.execute("SELECT * FROM step5_pro_forma").df()
    df_agg = df_raw.groupby('neighborhood_name')[['feasible_existing', 'new_pritzker', 'tot_true_sb79']].sum().reset_index()

    print(f"\n✅ Total Sandbox Runtime: {time.time() - global_start:.2f} seconds\n")
    print(df_agg.to_string(index=False))

    print("\n🔍 DIAGNOSTICS: Why are properties failing? ====================================")

    def get_failure_reason(row):
        if row['feasible_existing'] > 0: return '✅ Pass (Feasible)'
        if row['pass_zoning_class'] != True: return '❌ Failed: Invalid Zoning (OS/POS/PMD)'
        if row['pass_prop_class'] != True: return '❌ Failed: Invalid Prop Class (Condo/Exempt/Unknown)'
        if row['pass_min_value'] != True: return '❌ Failed: Tax Value < $1k (Anomaly/Sliver)'
        if row['pass_age_value'] != True: return '❌ Failed: Building Too New or Valuable'
        if row['pass_max_units'] != True: return '❌ Failed: Too Many Existing Units (>40)'
        if row['pass_unit_mult'] != True: return '❌ Failed: 2x Unit Multiplier Check'
        if row['pass_lot_density'] != True: return '❌ Failed: Existing Lot Already Too Dense / Too Large'
        if row['pass_sqft_mult'] != True: return '❌ Failed: 1.25x SqFt Check'
        if row['pass_financial_existing'] != True: return '❌ Failed: Not Profitable (Pro Forma ROI)'
        return '❌ Failed: Other'

    df_raw['status'] = df_raw.apply(get_failure_reason, axis=1)
    status_counts = df_raw['status'].value_counts()

    print(f"Total Parcels Evaluated: {len(df_raw):,}\n")
    for status, count in status_counts.items():
        print(f"{count:7,d} parcels -> {status}")

    print("\n📊 DEBUG LOG: Calculated Neighborhood Condo Prices ================================")
    df_condos = con.execute("SELECT * FROM dynamic_condo_values").df()
    print(df_condos.to_string(index=False))

    print("\n" + "="*80)
    print("🔍 SCENARIO ANALYSIS SAMPLES")
    print("="*80)

    def print_financial_block(row, scenario):
        clean_addr = str(row['prop_address']).title() if pd.notnull(row['prop_address']) else 'Unknown Address'
        assessed_val = row['tot_bldg_value'] + row['tot_land_value']
        sell_price = row['value_per_new_unit']
        margin = row['target_profit_margin']

        def calc_financials(capacity, cpu):
            rev = capacity * sell_price
            cost = (row['acquisition_cost'] + (capacity * cpu)) * margin
            return rev, cost

        def get_fail_reason(cap, cpu):
            if cap < max(1.0, row['existing_units']) * 2.0:
                return f"Fails 2x density minimum (Need {max(1.0, row['existing_units']) * 2.0}, got {cap})"
            rev, cost = calc_financials(cap, cpu)
            if rev <= cost:
                return f"Fails ROI (Cost: ${cost:,.0f} > Rev: ${rev:,.0f})"
            return "Fails secondary physical constraint (sqft minimum or lot density)"

        print(f"\n📍 {row['neighborhood_name']} | {clean_addr} | Zone: {row['zone_class']} | Area: {row['area_sqft']:,.0f} sqft")
        print(f"   🏠 EXISTING: {row['existing_units']} units | Age: {row['building_age']} yrs | Sqft: {row['existing_sqft']} | Class: {row['primary_prop_class']}")
        print(f"   📊 ACQUISITION: ${row['acquisition_cost']:,.0f} (Market Mult: {row['market_correction_multiplier']:.2f}x on Tax Val: ${assessed_val:,.0f})")
        print(f"   📈 NEW UNITS: Projected Sell Price: ${sell_price:,.0f} per unit")

        if scenario == 'ROI_FAIL':
            cap = row['current_capacity']
            rev, cost = calc_financials(cap, row['cpu_current'])
            print(f"   ❌ CURRENT ZONING FAILED: Allowed {cap} units.")
            print(f"      Construction: ${(cap*row['cpu_current']):,.0f} | Total Cost (w/ {margin}x margin): ${cost:,.0f}")
            print(f"      Expected Revenue: ${rev:,.0f}  <-- FAILED ROI")

        elif scenario == 'CURRENT_PASS':
            cap = row['current_capacity']
            rev, cost = calc_financials(cap, row['cpu_current'])
            print(f"   ✅ CURRENT ZONING PASSED: Allows {cap} units.")
            print(f"      Construction: ${(cap*row['cpu_current']):,.0f} | Total Cost (w/ {margin}x margin): ${cost:,.0f}")
            print(f"      Expected Revenue: ${rev:,.0f}  <-- PROFITABLE")

        elif scenario == 'PRITZKER_PASS':
            cap_curr = row['current_capacity']
            print(f"   ❌ CURRENT ZONING FAILED: Allowed {cap_curr} units.")
            print(f"      Reason: {get_fail_reason(cap_curr, row['cpu_current'])}")

            cap_pritzker = row['pritzker_capacity']
            rev, cost = calc_financials(cap_pritzker, row['cpu_pritzker'])
            print(f"   ✅ PRITZKER UPZONING PASSED: Allows {cap_pritzker} units.")
            print(f"      Construction: ${(cap_pritzker*row['cpu_pritzker']):,.0f} | Total Cost (w/ {margin}x margin): ${cost:,.0f}")
            print(f"      Expected Revenue: ${rev:,.0f}  <-- PROFITABLE")

        elif scenario == 'SB79_PASS':
            cap_pritzker = row['pritzker_capacity']
            print(f"   ❌ PRITZKER UPZONING FAILED: Allowed {cap_pritzker} units.")
            print(f"      Reason: {get_fail_reason(cap_pritzker, row['cpu_pritzker'])}")

            cap_sb79 = row['cap_true_sb79']
            rev, cost = calc_financials(cap_sb79, row['cpu_sb79'])
            print(f"   🚆 SB79 TOD UPZONING PASSED: Allows {cap_sb79} units.")
            print(f"      Construction: ${(cap_sb79*row['cpu_sb79']):,.0f} | Total Cost (w/ {margin}x margin): ${cost:,.0f}")
            print(f"      Expected Revenue: ${rev:,.0f}  <-- PROFITABLE")

        print("-" * 80)

    # 1. Failing strictly due to ROI
    print("\n[SCENARIO 1] Failing strictly due to ROI (Current Zoning)")
    df_failed_roi = df_raw[df_raw['status'] == '❌ Failed: Not Profitable (Pro Forma ROI)']
    if df_failed_roi.empty:
        print("No properties failed strictly due to ROI.")
    else:
        for _, row in df_failed_roi.groupby('neighborhood_name').head(1).iterrows():
            print_financial_block(row, 'ROI_FAIL')

    # 2. Passing under current laws
    print("\n[SCENARIO 2] Feasible Under CURRENT Laws")
    df_curr_pass = df_raw[df_raw['feasible_existing'] > 0]
    if df_curr_pass.empty:
        print("No properties pass under current laws.")
    else:
        for _, row in df_curr_pass.groupby('neighborhood_name').head(1).iterrows():
            print_financial_block(row, 'CURRENT_PASS')

    # 3. Passing ONLY under Pritzker
    print("\n[SCENARIO 3] Feasible ONLY under Pritzker Upzoning")
    df_pritzker_pass = df_raw[(df_raw['new_pritzker'] > 0) & (df_raw['feasible_existing'] == 0)]
    if df_pritzker_pass.empty:
        print("No properties pass exclusively under Pritzker upzoning.")
    else:
        for _, row in df_pritzker_pass.groupby('neighborhood_name').head(1).iterrows():
            print_financial_block(row, 'PRITZKER_PASS')

    # 4. Passing ONLY under SB79
    print("\n[SCENARIO 4] Feasible ONLY under True CA SB79 Transit Upzoning")
    df_sb79_pass = df_raw[(df_raw['add_true_sb79'] > 0) & (df_raw['new_pritzker'] == 0) & (df_raw['feasible_existing'] == 0)]
    if df_sb79_pass.empty:
        print("No properties pass exclusively under SB79 transit upzoning.")
    else:
        for _, row in df_sb79_pass.groupby('neighborhood_name').head(1).iterrows():
            print_financial_block(row, 'SB79_PASS')

    con.close()

if __name__ == "__main__":
    run_sandbox()
