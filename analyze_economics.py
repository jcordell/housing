import duckdb
import pandas as pd
import os

DB_FILE = "sb79_housing.duckdb"

def run_analysis():
    print("Loading cached dataset from DuckDB...")
    con = duckdb.connect(DB_FILE)
    try:
        df_neighborhoods = con.execute("SELECT * FROM neighborhood_results ORDER BY tot_train_and_bus_combo DESC").df()
    except Exception:
        print("❌ ERROR: Cached table not found. Please run with --recalculate flag first.")
        con.close()
        return None, None
    con.close()

    if df_neighborhoods.empty:
        print("No data found.")
        return None, None

    # Zillow Rent Data Processing
    high_cost_nbhds = []
    try:
        if os.path.exists('zillow_rent.csv'):
            df_rent = pd.read_csv('zillow_rent.csv')
            df_chi_rent = df_rent[df_rent['City'] == 'Chicago'].copy()
            df_chi_rent['neighborhood_name'] = df_chi_rent['RegionName'].str.upper().str.replace('LAKEVIEW', 'LAKE VIEW')

            date_cols = [c for c in df_chi_rent.columns if c.startswith('20')]
            if len(date_cols) >= 61:
                latest_col = date_cols[-1]
                five_yr_col = date_cols[-61]
                df_chi_rent['rent_increase_pct'] = ((df_chi_rent[latest_col] - df_chi_rent[five_yr_col]) / df_chi_rent[five_yr_col]) * 100

                valid_nbhds = df_neighborhoods['neighborhood_name'].unique()
                df_chi_rent = df_chi_rent[df_chi_rent['neighborhood_name'].isin(valid_nbhds)]
                high_cost_nbhds = df_chi_rent.nlargest(15, 'rent_increase_pct')['neighborhood_name'].tolist()
    except Exception as e:
        print("Could not process Zillow rent data:", e)

    if not high_cost_nbhds:
        high_cost_nbhds = ['LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'NEAR WEST SIDE', 'NORTH CENTER', 'WEST TOWN', 'LOGAN SQUARE', 'EDGEWATER', 'LINCOLN SQUARE']

    # Slicing data
    df_top15 = df_neighborhoods[df_neighborhoods['neighborhood_name'].isin(high_cost_nbhds[:15])]
    df_top5 = df_neighborhoods[df_neighborhoods['neighborhood_name'].isin(high_cost_nbhds[:5])]
    df_rest = df_neighborhoods[~df_neighborhoods['neighborhood_name'].isin(high_cost_nbhds[:15])]

    feasible_existing = df_neighborhoods['feasible_existing'].sum()
    exp_pritzker = df_top15['new_pritzker'].sum()
    exp_sb79_full = df_top15['tot_true_sb79'].sum()
    exp_sb79_diff = df_top15['add_true_sb79'].sum()

    top5_pritzker = df_top5['new_pritzker'].sum()
    top5_sb79_full = df_top5['tot_true_sb79'].sum()

    pct_pritzker = (exp_pritzker / df_neighborhoods['new_pritzker'].sum()) * 100 if df_neighborhoods['new_pritzker'].sum() > 0 else 0
    pct_sb79 = (exp_sb79_full / df_neighborhoods['tot_true_sb79'].sum()) * 100 if df_neighborhoods['tot_true_sb79'].sum() > 0 else 0

    top5_pct_sqft = (df_top5['area_mf_zoned'].sum() / df_top5['total_area_sqft'].sum()) * 100 if df_top5['total_area_sqft'].sum() > 0 else 0
    rest_pct_sqft = (df_rest['area_mf_zoned'].sum() / df_rest['total_area_sqft'].sum()) * 100 if df_rest['total_area_sqft'].sum() > 0 else 0
    pct_top15_area = (df_top15['total_area_sqft'].sum() / df_neighborhoods['total_area_sqft'].sum()) * 100 if df_neighborhoods['total_area_sqft'].sum() > 0 else 0

    avg_sfh_value = 1200000
    avg_condo_value = 450000
    effective_tax_rate = 0.018

    sfh_tax_per_unit = avg_sfh_value * effective_tax_rate
    unit_tax_per_condo = avg_condo_value * effective_tax_rate

    sfh_yield_per_acre = 14 * sfh_tax_per_unit
    four_flat_yield_per_acre = (14 * 4) * unit_tax_per_condo
    midrise_yield_per_acre = 100 * unit_tax_per_condo

    tax_multiplier = midrise_yield_per_acre / sfh_yield_per_acre

    print("\n" + "="*80)
    print("HOUSING POLICY IMPACT ANALYSIS: (Filtered for Redevelopment Feasibility)")
    print("="*80)
    print(f"0. Status Quo (Currently Feasible):              {feasible_existing:,.0f}")
    print(f"1. Original Pritzker Upzoning (Net New):         {df_neighborhoods['new_pritzker'].sum():,.0f}")
    print("-" * 80)
    print(f"2. TRUE CA SB 79 (Trains + BRT/Bus Intersections): {df_neighborhoods['tot_true_sb79'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_true_sb79'].sum():,.0f}")
    print(f"3. SB 79: Trains Only:                           {df_neighborhoods['tot_train_only'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_only'].sum():,.0f}")
    print(f"4. SB 79 TRAIN + (HIGH FREQ BUS OR 2+ BUS LINES): {df_neighborhoods['tot_train_and_bus_combo'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}")
    print(f"5. SB 79 TRAIN + HIGH FREQ BUS:                  {df_neighborhoods['tot_train_and_hf_bus'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}")
    print("="*80 + "\n")

    template_data = {
        'feasible_existing': f"{feasible_existing:,.0f}",
        'pritzker_total': f"{df_neighborhoods['new_pritzker'].sum():,.0f}",
        'pct_pritzker': f"{pct_pritzker:.1f}",
        'true_sb79_total': f"{df_neighborhoods['tot_true_sb79'].sum():,.0f}",
        'true_sb79_diff': f"+{df_neighborhoods['add_true_sb79'].sum():,.0f}",
        'pct_sb79': f"{pct_sb79:.1f}",
        'train_only_total': f"{df_neighborhoods['tot_train_only'].sum():,.0f}",
        'train_only_diff': f"+{df_neighborhoods['add_train_only'].sum():,.0f}",
        'train_combo_total': f"{df_neighborhoods['tot_train_and_bus_combo'].sum():,.0f}",
        'train_combo_diff': f"+{df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}",
        'train_hf_total': f"{df_neighborhoods['tot_train_and_hf_bus'].sum():,.0f}",
        'train_hf_diff': f"+{df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}",
        'exp_sb79_diff': f"{exp_sb79_diff:,.0f}",
        'affordable_units': f"{exp_sb79_diff * 0.20:,.0f}",
        'top5_pct_sqft': f"{top5_pct_sqft:.1f}",
        'rest_pct_sqft': f"{rest_pct_sqft:.1f}",
        'pct_top15_area': f"{pct_top15_area:.1f}",
        'top5_pritzker': f"{top5_pritzker:,.0f}",
        'top5_sb79_full': f"{top5_sb79_full:,.0f}",
        'sfh_yield': f"${sfh_yield_per_acre:,.0f}",
        'four_flat_yield': f"${four_flat_yield_per_acre:,.0f}",
        'midrise_yield': f"${midrise_yield_per_acre:,.0f}",
        'tax_multiplier': f"{tax_multiplier:.1f}"
    }

    return df_neighborhoods, template_data
