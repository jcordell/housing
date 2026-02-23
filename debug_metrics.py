import duckdb
import pandas as pd
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_debug_metrics():
    config = load_config()
    db_file = config['database']['file_name']

    con = duckdb.connect(db_file)

    print("\n" + "="*80)
    print("1. THE FILTER FUNNEL: Where are properties failing?")
    print("="*80)

    funnel_query = """
    SELECT
        COUNT(*) as total_parcels,
        SUM(CAST(pass_zoning_class AS INT)) as pass_zoning,
        SUM(CAST(pass_zoning_class AND pass_prop_class AS INT)) as pass_prop_type,
        SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AS INT)) as pass_age_and_value,
        SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_unit_mult AND pass_sqft_mult AND pass_lot_density AS INT)) as pass_physical_growth,
        SUM(CAST(pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_unit_mult AND pass_sqft_mult AND pass_lot_density AND pass_financial_existing AS INT)) as fully_feasible_status_quo
    FROM step5_pro_forma
    """
    df_funnel = con.execute(funnel_query).df()

    total = df_funnel['total_parcels'][0]
    print(f"Total Parcels Evaluated: {total:,}")
    print(f"  ↳ Survived Zoning (No Parks/Mfg): {df_funnel['pass_zoning'][0]:,} ({(df_funnel['pass_zoning'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Prop Type (No Condos/Exempt): {df_funnel['pass_prop_type'][0]:,} ({(df_funnel['pass_prop_type'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Age/Value (Old/Cheap enough): {df_funnel['pass_age_and_value'][0]:,} ({(df_funnel['pass_age_and_value'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Physical Limits (Has room to grow): {df_funnel['pass_physical_growth'][0]:,} ({(df_funnel['pass_physical_growth'][0]/total)*100:.1f}%)")
    print(f"  ↳ Survived Financial ROI (Status Quo limits): {df_funnel['fully_feasible_status_quo'][0]:,} ({(df_funnel['fully_feasible_status_quo'][0]/total)*100:.1f}%)")

    print("\n" + "="*80)
    print("1B. PROPERTY TYPE DIAGNOSTICS (Diagnosing the Drop-off)")
    print("="*80)

    prop_debug_query = """
    SELECT
        primary_prop_class,
        COUNT(*) as count,
        CASE
            WHEN primary_prop_class LIKE '299%' THEN 'Condo'
            WHEN primary_prop_class = 'EX' THEN 'Exempt/Government'
            WHEN primary_prop_class = 'UNKNOWN' THEN 'Join Failure/No Data'
            WHEN primary_prop_class IN ('202','203','204','205','206','207','208','209','210', '234', '278') THEN 'Single Family'
            WHEN primary_prop_class IN ('211','212') THEN '2-to-3 Flat'
            WHEN primary_prop_class IN ('213','214') THEN 'Multi-Family (4+ Units)'
            WHEN primary_prop_class LIKE '3%' THEN 'Commercial Multi-Family'
            WHEN primary_prop_class LIKE '5%' THEN 'Commercial General'
            ELSE 'Other'
        END as category,
        CAST(pass_prop_class AS INT) as did_pass_filter
    FROM step5_pro_forma
    WHERE pass_zoning_class = true
    GROUP BY 1, 3, 4
    ORDER BY count DESC
    LIMIT 20
    """
    df_prop = con.execute(prop_debug_query).df()
    print(df_prop.to_string(index=False))

    print("\n" + "="*80)
    print("2. NEIGHBORHOOD ECONOMICS: The Appraisal Gap vs. Expensive Dirt")
    print("="*80)
    print("(Showing lots that passed physical/age filters, but checking why they failed financially)")

    econ_query = """
    SELECT
        neighborhood_name,
        COUNT(*) as candidate_lots,
        SUM(CAST(NOT pass_financial_existing AS INT)) as failed_roi_count,
        ROUND((SUM(CAST(NOT pass_financial_existing AS INT)) * 100.0) / COUNT(*), 1) as fail_rate_pct,
        CAST(MEDIAN(condo_price_per_sqft) AS INT) as med_condo_price_sqft,
        CAST(MEDIAN(acquisition_cost) AS INT) as med_acq_cost,
        CAST(MEDIAN(cpu_current * current_capacity) AS INT) as med_const_cost,
        CAST(MEDIAN(value_per_new_unit * current_capacity) AS INT) as med_projected_rev
    FROM step5_pro_forma
    WHERE pass_zoning_class AND pass_prop_class AND pass_min_value AND pass_age_value AND pass_max_units AND pass_unit_mult AND pass_sqft_mult AND pass_lot_density
    GROUP BY neighborhood_name
    HAVING COUNT(*) > 10
    ORDER BY failed_roi_count DESC
    LIMIT 20
    """
    df_econ = con.execute(econ_query).df()
    print(df_econ.to_string(index=False, formatters={
        'med_acq_cost': '${:,.0f}'.format,
        'med_const_cost': '${:,.0f}'.format,
        'med_projected_rev': '${:,.0f}'.format,
        'med_condo_price_sqft': '${:,.0f}'.format
    }))

    print("\n" + "="*80)
    print("3. ZONING BOTTLENECK: Which zones produce the most Status Quo units?")
    print("="*80)

    zoning_query = """
    SELECT
        zone_class,
        COUNT(*) as total_parcels,
        SUM(CAST(feasible_existing > 0 AS INT)) as feasible_parcels,
        SUM(feasible_existing) as new_units_yielded
    FROM step5_pro_forma
    WHERE pass_zoning_class AND pass_prop_class
    GROUP BY zone_class
    ORDER BY new_units_yielded DESC
    LIMIT 10
    """
    df_zoning = con.execute(zoning_query).df()
    print(df_zoning.to_string(index=False))

    print("\n" + "="*80)
    print("4. SB79 TOD IMPACT: Where does transit density suddenly make the math work?")
    print("="*80)

    tod_query = """
    SELECT
        neighborhood_name,
        SUM(CAST(feasible_existing > 0 AS INT)) as status_quo_parcels,
        SUM(CAST(add_true_sb79 > 0 AND feasible_existing = 0 AND new_pritzker = 0 AS INT)) as newly_viable_parcels_under_sb79,
        SUM(add_true_sb79) as net_new_units_from_sb79
    FROM step5_pro_forma
    GROUP BY neighborhood_name
    HAVING SUM(add_true_sb79) > 0
    ORDER BY net_new_units_from_sb79 DESC
    LIMIT 15
    """
    df_tod = con.execute(tod_query).df()
    print(df_tod.to_string(index=False))

    con.close()

if __name__ == "__main__":
    run_debug_metrics()
