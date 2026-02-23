import duckdb
import pandas as pd
import yaml
import sys

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_debug_metrics():
    config = load_config()
    db_file = config['database']['file_name']

    con = duckdb.connect(db_file)

    print("\n" + "="*80)
    print("1. THE FILTER FUNNEL: Separating Legality from Profitability")
    print("="*80)

    # UPDATED: Using raw legal checks to prevent financial masking
    funnel_query = """
                   SELECT
                       COUNT(*) as total_parcels,
                       SUM(CAST(pass_zoning_class AS INT)) as pass_zoning,
                       SUM(CAST(pass_zoning_class AND pass_prop_class AS INT)) as pass_prop_type,
                       SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AS INT)) as pass_age_and_value,
                       SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density AS INT)) as pass_physical_size,
                       SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density AND pass_unit_mult_raw AS INT)) as survived_legal_bottleneck,
                       SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_lot_density AND pass_unit_mult_raw AND pass_financial_existing AS INT)) as fully_feasible_status_quo
                   FROM step5_pro_forma \
                   """
    df_funnel = con.execute(funnel_query).df()

    total = df_funnel['total_parcels'][0]
    print(f"Total Parcels Evaluated: {total:,}")
    print(f"  ↳ Survived Zoning (No Parks/Mfg): {df_funnel['pass_zoning'][0]:,} ({(df_funnel['pass_zoning'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Prop Type (Residential): {df_funnel['pass_prop_type'][0]:,} ({(df_funnel['pass_prop_type'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Age/Value (Old/Cheap):     {df_funnel['pass_age_and_value'][0]:,} ({(df_funnel['pass_age_and_value'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Physical (Under 1 Acre):   {df_funnel['pass_physical_size'][0]:,} ({(df_funnel['pass_physical_size'][0]/total)*100:.1f}%)")
    print("-" * 30)
    print(f"  ↳ THE ZONING BOTTLENECK:              {df_funnel['survived_legal_bottleneck'][0]:,} ({(df_funnel['survived_legal_bottleneck'][0]/total)*100:.1f}%)")
    print("    (Lots legally allowed to double density today)")
    print("-" * 30)
    print(f"  ↳ THE APPRAISAL GAP:                  {df_funnel['fully_feasible_status_quo'][0]:,} ({(df_funnel['fully_feasible_status_quo'][0]/total)*100:.1f}%)")
    print("    (Lots both legal AND profitable to build today)")

    # ---------------------------------------------------------
    # REST OF SCRIPT REMAINS THE SAME (Sections 1B - 7)
    # ---------------------------------------------------------

    print("\n" + "="*80)
    print("1B. PROPERTY TYPE DIAGNOSTICS (Diagnosing the Drop-off)")
    print("="*80)
    prop_debug_query = """
                       SELECT primary_prop_class, COUNT(*) as count, 
    CASE WHEN primary_prop_class LIKE '299%' THEN 'Condo' WHEN primary_prop_class = 'EX' THEN 'Exempt/Government' WHEN primary_prop_class = 'UNKNOWN' THEN 'Join Failure/No Data' WHEN primary_prop_class IN ('202','203','204','205','206','207','208','209','210', '234', '278') THEN 'Single Family' WHEN primary_prop_class IN ('211','212') THEN '2-to-3 Flat' WHEN primary_prop_class IN ('213','214') THEN 'Multi-Family (4+ Units)' WHEN primary_prop_class LIKE '3%' THEN 'Commercial Multi-Family' WHEN primary_prop_class LIKE '5%' THEN 'Commercial General' ELSE 'Other' END as category,
    CAST(pass_prop_class AS INT) as did_pass_filter FROM step5_pro_forma WHERE pass_zoning_class = true GROUP BY 1, 3, 4 ORDER BY count DESC LIMIT 20 \
                       """
    print(con.execute(prop_debug_query).df().to_string(index=False))

    print("\n" + "="*80)
    print("2. NEIGHBORHOOD ECONOMICS: The Appraisal Gap vs. Expensive Dirt")
    print("="*80)
    econ_query = """
                 SELECT neighborhood_name, COUNT(*) as candidate_lots, SUM(CAST(NOT pass_financial_existing AS INT)) as failed_roi_count, ROUND((SUM(CAST(NOT pass_financial_existing AS INT)) * 100.0) / COUNT(*), 1) as fail_rate_pct, CAST(MEDIAN(condo_price_per_sqft) AS INT) as med_condo_price_sqft, CAST(MEDIAN(acquisition_cost) AS INT) as med_acq_cost, CAST(MEDIAN(cpu_current * current_capacity) AS INT) as med_const_cost, CAST(MEDIAN(value_per_new_unit * current_capacity) AS INT) as med_projected_rev FROM step5_pro_forma WHERE pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_unit_mult AND pass_sqft_mult AND pass_lot_density GROUP BY neighborhood_name HAVING COUNT(*) > 10 ORDER BY failed_roi_count DESC LIMIT 20 \
                 """
    print(con.execute(econ_query).df().to_string(index=False, formatters={'med_acq_cost': '${:,.0f}'.format, 'med_const_cost': '${:,.0f}'.format, 'med_projected_rev': '${:,.0f}'.format, 'med_condo_price_sqft': '${:,.0f}'.format}))

    print("\n" + "="*80)
    print("3. ZONING BOTTLENECK: Which zones produce the most Status Quo units?")
    print("="*80)
    zoning_query = "SELECT zone_class, COUNT(*) as total_parcels, SUM(CAST(feasible_existing > 0 AS INT)) as feasible_parcels, SUM(feasible_existing) as new_units_yielded FROM step5_pro_forma WHERE pass_zoning_class AND pass_prop_class GROUP BY zone_class ORDER BY new_units_yielded DESC LIMIT 10"
    print(con.execute(zoning_query).df().to_string(index=False))

    print("\n" + "="*80)
    print("4. SB79 TOD IMPACT: Where does transit density suddenly make the math work?")
    print("="*80)
    tod_query = "SELECT neighborhood_name, SUM(CAST(feasible_existing > 0 AS INT)) as status_quo_parcels, SUM(CAST(add_true_sb79 > 0 AND feasible_existing = 0 AND new_pritzker = 0 AS INT)) as newly_viable_parcels_under_sb79, SUM(add_true_sb79) as net_new_units_from_sb79 FROM step5_pro_forma GROUP BY neighborhood_name HAVING SUM(add_true_sb79) > 0 ORDER BY net_new_units_from_sb79 DESC LIMIT 15"
    print(con.execute(tod_query).df().to_string(index=False))

    print("\n" + "="*80)
    print("5. DEBUG SAMPLES (CSV FORMAT)")
    print("="*80)
    sample_query = """
                   WITH status_quo AS (SELECT 'Passes Status Quo' as scenario, * FROM step5_pro_forma WHERE feasible_existing > 0 LIMIT 2),
                       pritzker_only AS (SELECT 'Passes Pritzker Only' as scenario, * FROM step5_pro_forma WHERE feasible_existing = 0 AND new_pritzker > 0 LIMIT 3),
                       sb79_only AS (SELECT 'Passes SB79 Only' as scenario, * FROM step5_pro_forma WHERE feasible_existing = 0 AND new_pritzker = 0 AND add_true_sb79 > 0 LIMIT 3),
                       fails_all AS (SELECT 'Fails All (ROI or Physical)' as scenario, * FROM step5_pro_forma WHERE feasible_existing = 0 AND new_pritzker = 0 AND add_true_sb79 = 0 AND pass_zoning_class = true AND pass_prop_class = true LIMIT 2)
                   SELECT scenario, prop_address, neighborhood_name, zone_class, area_sqft, primary_prop_class, existing_units, building_age, market_correction_multiplier, condo_price_per_sqft, value_per_new_unit, acquisition_cost, cpu_current as base_construction_cost_per_unit, current_capacity, pritzker_capacity, cap_true_sb79, feasible_existing, new_pritzker, add_true_sb79 FROM status_quo
                   UNION ALL SELECT scenario, prop_address, neighborhood_name, zone_class, area_sqft, primary_prop_class, existing_units, building_age, market_correction_multiplier, condo_price_per_sqft, value_per_new_unit, acquisition_cost, cpu_current, current_capacity, pritzker_capacity, cap_true_sb79, feasible_existing, new_pritzker, add_true_sb79 FROM pritzker_only
                   UNION ALL SELECT scenario, prop_address, neighborhood_name, zone_class, area_sqft, primary_prop_class, existing_units, building_age, market_correction_multiplier, condo_price_per_sqft, value_per_new_unit, acquisition_cost, cpu_current, current_capacity, pritzker_capacity, cap_true_sb79, feasible_existing, new_pritzker, add_true_sb79 FROM sb79_only
                   UNION ALL SELECT scenario, prop_address, neighborhood_name, zone_class, area_sqft, primary_prop_class, existing_units, building_age, market_correction_multiplier, condo_price_per_sqft, value_per_new_unit, acquisition_cost, cpu_current, current_capacity, pritzker_capacity, cap_true_sb79, feasible_existing, new_pritzker, add_true_sb79 FROM fails_all \
                   """
    con.execute(sample_query).df().to_csv(sys.stdout, index=False)

    print("\n" + "="*80)
    print("6. LAKE VIEW & LINCOLN PARK SCENARIO SAMPLES")
    print("="*80)
    lv_lp_query = """
                  WITH lv_sq AS (SELECT 'LV - Status Quo' as scenario, * FROM step5_pro_forma WHERE neighborhood_name = 'LAKE VIEW' AND feasible_existing > 0 ORDER BY RANDOM() LIMIT 10),
                      lp_sq AS (SELECT 'LP - Status Quo' as scenario, * FROM step5_pro_forma WHERE neighborhood_name = 'LINCOLN PARK' AND feasible_existing > 0 ORDER BY RANDOM() LIMIT 10),
                      lv_pritzker AS (SELECT 'LV - Pritzker Only' as scenario, * FROM step5_pro_forma WHERE neighborhood_name = 'LAKE VIEW' AND feasible_existing = 0 AND new_pritzker > 0 ORDER BY RANDOM() LIMIT 10),
                      lp_pritzker AS (SELECT 'LP - Pritzker Only' as scenario, * FROM step5_pro_forma WHERE neighborhood_name = 'LINCOLN PARK' AND feasible_existing = 0 AND new_pritzker > 0 ORDER BY RANDOM() LIMIT 10),
                      lv_sb79 AS (SELECT 'LV - SB79 Only' as scenario, * FROM step5_pro_forma WHERE neighborhood_name = 'LAKE VIEW' AND feasible_existing = 0 AND new_pritzker = 0 AND add_true_sb79 > 0 ORDER BY RANDOM() LIMIT 10),
                      lp_sb79 AS (SELECT 'LP - SB79 Only' as scenario, * FROM step5_pro_forma WHERE neighborhood_name = 'LINCOLN PARK' AND feasible_existing = 0 AND new_pritzker = 0 AND add_true_sb79 > 0 ORDER BY RANDOM() LIMIT 10),
                      combined_samples AS (SELECT * FROM lv_sq UNION ALL SELECT * FROM lp_sq UNION ALL SELECT * FROM lv_pritzker UNION ALL SELECT * FROM lp_pritzker UNION ALL SELECT * FROM lv_sb79 UNION ALL SELECT * FROM lp_sb79)
                  SELECT scenario, prop_address, neighborhood_name, zone_class, area_sqft, primary_prop_class, existing_units, building_age, market_correction_multiplier, condo_price_per_sqft, value_per_new_unit, acquisition_cost, cpu_current as base_construction_cost_per_unit, current_capacity, pritzker_capacity, cap_true_sb79, feasible_existing, new_pritzker, add_true_sb79 FROM combined_samples \
                  """
    con.execute(lv_lp_query).df().to_csv(sys.stdout, index=False)

    print("\n" + "="*80)
    print("7. SOUTH SIDE PROFITABILITY CHECK")
    print("="*80)
    ss_query = """
               WITH ss AS (SELECT * FROM step5_pro_forma WHERE neighborhood_name IN ('ENGLEWOOD', 'WEST ENGLEWOOD', 'WOODLAWN', 'WASHINGTON PARK', 'CHATHAM', 'AUBURN GRESHAM', 'SOUTH SHORE', 'ROSELAND', 'PULLMAN', 'GREATER GRAND CROSSING', 'BRONZEVILLE', 'SOUTH CHICAGO') AND pass_zoning_class AND pass_prop_class),
                    ss_c AS (SELECT 'SQ' as scenario, ((current_capacity * value_per_new_unit) / NULLIF(acquisition_cost + (current_capacity * cpu_current), 0)) as r, * FROM ss WHERE current_capacity > existing_units ORDER BY r DESC LIMIT 10),
                   ss_p AS (SELECT 'PR' as scenario, ((pritzker_capacity * value_per_new_unit) / NULLIF(acquisition_cost + (pritzker_capacity * cpu_current), 0)) as r, * FROM ss WHERE pritzker_capacity > current_capacity ORDER BY r DESC LIMIT 10),
                   ss_s AS (SELECT 'SB' as scenario, ((cap_true_sb79 * value_per_new_unit) / NULLIF(acquisition_cost + (cap_true_sb79 * cpu_current), 0)) as r, * FROM ss WHERE cap_true_sb79 > GREATEST(current_capacity, pritzker_capacity) ORDER BY r DESC LIMIT 10)
               SELECT scenario, prop_address, neighborhood_name, zone_class, area_sqft, primary_prop_class, existing_units, acquisition_cost, cpu_current, value_per_new_unit, current_capacity, pritzker_capacity, cap_true_sb79, ROUND(r, 3) as raw_roi_ratio FROM (SELECT * FROM ss_c UNION ALL SELECT * FROM ss_p UNION ALL SELECT * FROM ss_s) \
               """
    print(con.execute(ss_query).df().to_csv(sys.stdout, index=False))
    con.close()

if __name__ == "__main__":
    run_debug_metrics()
