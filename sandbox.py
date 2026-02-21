import duckdb
import time
from financial_model import run_spatial_pipeline

DB_FILE = "sb79_housing.duckdb"

def run_sandbox():
    global_start = time.time()
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("PRAGMA enable_progress_bar;")

    print("\n🚀 Running Rapid Sandbox Analysis (Lincoln Park, Lake View, Austin, Ashburn)...")

    # Delegate all heavy lifting to the centralized financial model pipeline
    run_spatial_pipeline(con, is_sandbox=True)

    # Print the aggregate results from the temporary table
    df_raw = con.execute("SELECT * FROM step5_pro_forma").df()
    df_agg = df_raw.groupby('neighborhood_name')[['feasible_existing', 'new_pritzker', 'add_true_sb79']].sum().reset_index()

    print(f"\n✅ Total Sandbox Runtime: {time.time() - global_start:.2f} seconds\n")
    print(df_agg.to_string(index=False))

    # Print the detailed property debug logs
    print("\n🔍 DEBUG LOG: Sample of properties marked 'feasible_existing' ====================================\n")
    df_feasible = df_raw[df_raw['feasible_existing'] > 0]

    if df_feasible.empty:
        print("No feasible properties found! Check your constraints.")
    else:
        sample_df = df_feasible.groupby('neighborhood_name').head(2)
        for _, row in sample_df.iterrows():
            clean_addr = str(row['prop_address']).title() if pd.notnull(row['prop_address']) else 'Unknown Address'
            assessed_val = row['tot_bldg_value'] + row['tot_land_value']

            print(f"📍 {row['neighborhood_name']} | {clean_addr} | Zone: {row['zone_class']} | Area: {row['area_sqft']:,.0f} sqft")
            print(f"   🏠 EXISTING: {row['existing_units']} units | Age: {row['building_age']} yrs | Sqft: {row['existing_sqft']} | Class: {row['primary_prop_class']}")
            print(f"   📈 PROPOSED: {row['current_capacity']} units | Value/Unit: ${row['value_per_new_unit']:,.0f} (Rent: ${row['local_rent']:,.0f}/mo)")

            cpu = row['cost_per_unit_low_density']
            profit_margin = row['target_profit_margin']
            total_revenue = row['current_capacity'] * row['value_per_new_unit']
            total_cost = (row['acquisition_cost'] + (row['current_capacity'] * cpu)) * profit_margin

            print(f"   📊 MARKET MULTIPLIER: {row['market_correction_multiplier']:.2f}x (Applied to Tax Assessed Value of ${assessed_val:,.0f})")
            print(f"   🏗️  EST. COST PER UNIT: ${cpu:,.0f} (Derived from neighborhood building permits)")
            print(f"   💰 MATH: Acq Cost: ${row['acquisition_cost']:,.0f} + Construction: ${(row['current_capacity']*cpu):,.0f} = Total Cost: ${total_cost:,.0f} (inc. profit)")
            print(f"            Expected Revenue: ${total_revenue:,.0f}")
            print("-" * 80)

    con.close()

if __name__ == "__main__":
    run_sandbox()
