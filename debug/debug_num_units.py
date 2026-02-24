import duckdb
import yaml
import pandas as pd

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def calculate_sb79_exclusive_units():
    config = load_config()
    db_file = config['database']['file_name']

    # Connect to the existing DuckDB database
    con = duckdb.connect(db_file)

    # ---------------------------------------------------------
    # SECTION 1: OVERALL UNIT DISTRIBUTION
    # ---------------------------------------------------------
    query_sizes = """
    WITH sb79_only AS (
        -- Isolate parcels that pencil out under SB79 but FAILED under Pritzker/Current
        SELECT yield_sb79 AS num_units
        FROM step5_pro_forma
        WHERE yield_sb79 > 0
          AND yield_pritzker = 0
    ),
    aggregated AS (
        -- Count how many buildings of each size exist
        SELECT num_units, COUNT(*) as total
        FROM sb79_only
        GROUP BY num_units
    ),
    totals AS (
        -- Get the grand total to calculate percentages
        SELECT SUM(total) as grand_total FROM aggregated
    )
    SELECT
        CAST(a.num_units AS INT) as num_units,
        a.total,
        ROUND(100.0 * a.total / t.grand_total, 2) AS perc
    FROM aggregated a
    CROSS JOIN totals t
    ORDER BY a.num_units;
    """

    df_sizes = con.execute(query_sizes).df()

    print("--- OVERALL SB79-EXCLUSIVE UNIT DISTRIBUTION ---")
    print("num_units,total,perc")
    for _, row in df_sizes.iterrows():
        print(f"{int(row['num_units'])},{int(row['total'])},{row['perc']}%")

    # ---------------------------------------------------------
    # SECTION 2: THE 4-UNIT BUILDING ORIGINS
    # ---------------------------------------------------------
    query_4_units = """
    WITH four_units AS (
        SELECT
            CASE
                WHEN zone_class LIKE 'RS%' THEN 'RS (Single-Family)'
                WHEN zone_class LIKE 'RT%' THEN 'RT (Two-Flat/Townhouse)'
                WHEN zone_class LIKE 'RM%' THEN 'RM (Multi-Family)'
                WHEN zone_class LIKE 'B%' THEN 'B (Business)'
                WHEN zone_class LIKE 'C%' THEN 'C (Commercial)'
                ELSE 'Other'
            END as base_zone,
            CASE
                WHEN zone_class LIKE 'B%' OR zone_class LIKE 'C%' THEN 'Zoning Inclusion (B/C zones ignored by Pritzker)'
                ELSE 'Financial Feasibility (FAR bumped from 1.5 to 3.0)'
            END as unlock_reason
        FROM step5_pro_forma
        WHERE yield_sb79 = 4
          AND yield_pritzker = 0
    ),
    aggregated AS (
        SELECT
            base_zone,
            unlock_reason,
            COUNT(*) as total
        FROM four_units
        GROUP BY base_zone, unlock_reason
    ),
    totals AS (
        SELECT SUM(total) as grand_total FROM aggregated
    )
    SELECT
        a.base_zone,
        a.unlock_reason,
        a.total,
        ROUND(100.0 * a.total / t.grand_total, 2) AS perc
    FROM aggregated a
    CROSS JOIN totals t
    ORDER BY a.total DESC;
    """

    df_4_units = con.execute(query_4_units).df()

    print("\n--- 4-UNIT BUILDING ORIGINS (SB79 EXCLUSIVE) ---")
    print("base_zone,unlock_reason,total,perc")
    for _, row in df_4_units.iterrows():
        print(f"{row['base_zone']},\"{row['unlock_reason']}\",{int(row['total'])},{row['perc']}%")

    # ---------------------------------------------------------
    # SECTION 3: EXAMPLE ADDRESSES & PROFITABILITY (LAKEVIEW/LINCOLN PARK)
    # ---------------------------------------------------------
    query_examples = """
    SELECT
        prop_address,
        neighborhood_name,
        zone_class,
        area_sqft,
        acquisition_cost,
        condo_price_per_sqft,
        rev_pritzker,
        cost_pritzker,
        (rev_pritzker / NULLIF(cost_pritzker, 0)) AS roi_pritzker,
        rev_sb79,
        cost_sb79,
        (rev_sb79 / NULLIF(cost_sb79, 0)) AS roi_sb79
    FROM step5_pro_forma
    WHERE yield_sb79 = 4
      AND yield_pritzker = 0
      AND zone_class LIKE 'RS%'
      AND prop_address IS NOT NULL
      AND neighborhood_name IN ('LAKE VIEW', 'LINCOLN PARK')
    LIMIT 10;
    """

    df_examples = con.execute(query_examples).df()

    print("\n--- 10 EXAMPLE ADDRESSES: SB79 FAR BUMP (LAKEVIEW & LINCOLN PARK) ---")
    print(f"{'Address':<25} | {'Neighborhood':<15} | {'SqFt':<6} | {'Acq Cost':<10} | {'PPSF':<5} | {'Pritzker ROI':<12} | {'SB79 ROI':<10}")
    print("-" * 105)

    for _, row in df_examples.iterrows():
        addr = str(row['prop_address']).title()[:24]
        nbhd = str(row['neighborhood_name']).title()[:14]
        sqft = f"{int(row['area_sqft'])}"
        acq = f"${int(row['acquisition_cost']):,}"
        ppsf = f"${int(row['condo_price_per_sqft'])}"

        # Format ROIs. Needs 1.15 to pass.
        roi_p = f"{row['roi_pritzker']:.3f} (FAIL)"
        roi_s = f"{row['roi_sb79']:.3f} (PASS)"

        print(f"{addr:<25} | {nbhd:<15} | {sqft:<6} | {acq:<10} | {ppsf:<5} | {roi_p:<12} | {roi_s:<10}")

    con.close()

if __name__ == "__main__":
    calculate_sb79_exclusive_units()

