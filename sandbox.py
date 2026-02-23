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

    print("\n🔍 DEBUG LOG: Sample of properties failing due to ROI =============================\n")
    df_failed_roi = df_raw[df_raw['status'] == '❌ Failed: Not Profitable (Pro Forma ROI)']

    if df_failed_roi.empty:
        print("No properties failed strictly due to ROI.")
    else:
        sample_df = df_failed_roi.groupby('neighborhood_name').head(2)
        for _, row in sample_df.iterrows():
            clean_addr = str(row['prop_address']).title() if pd.notnull(row['prop_address']) else 'Unknown Address'
            assessed_val = row['tot_bldg_value'] + row['tot_land_value']

            print(f"📍 {row['neighborhood_name']} | {clean_addr} | Zone: {row['zone_class']} | Area: {row['area_sqft']:,.0f} sqft")
            print(f"   🏠 EXISTING: {row['existing_units']} units | Age: {row['building_age']} yrs | Sqft: {row['existing_sqft']} | Class: {row['primary_prop_class']}")
            print(f"   📈 PROPOSED: {row['current_capacity']} units | Projected Condo Sell Price: ${row['value_per_new_unit']:,.0f} per unit")

            cpu = row['cpu_current']
            profit_margin = row['target_profit_margin']
            total_revenue = row['current_capacity'] * row['value_per_new_unit']
            total_cost = (row['acquisition_cost'] + (row['current_capacity'] * cpu)) * profit_margin

            print(f"   📊 MARKET MULTIPLIER: {row['market_correction_multiplier']:.2f}x (Applied to Tax Assessed Value of ${assessed_val:,.0f})")
            print(f"   💰 MATH: Acq Cost: ${row['acquisition_cost']:,.0f} + Construction: ${(row['current_capacity']*cpu):,.0f} = Total Cost: ${total_cost:,.0f} (inc. {profit_margin}x profit target)")
            print(f"            Expected Revenue: ${total_revenue:,.0f}   <-- FAILED ROI")
            print("-" * 80)

    con.close()

if __name__ == "__main__":
    run_sandbox()
