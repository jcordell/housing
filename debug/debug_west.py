import duckdb
import pandas as pd

def run_west_elsdon_debug():
    con = duckdb.connect('data/sb79_housing.duckdb')

    print("\n" + "="*110)
    print("1. MACRO SUMMARY: West Elsdon Yields")
    print("="*110)

    summary_query = """
    SELECT
        COUNT(*) as total_parcels,
        SUM(feasible_existing) as status_quo_new_units,
        SUM(new_pritzker) as pritzker_bonus_units,
        SUM(add_true_sb79) as sb79_bonus_units,
        SUM(feasible_existing + new_pritzker) as total_pritzker_units,
        SUM(feasible_existing + new_pritzker + add_true_sb79) as total_sb79_units
    FROM step5_pro_forma
    WHERE neighborhood_name = 'WEST ELSDON'
    """
    df_sum = con.execute(summary_query).df()
    for col in df_sum.columns:
        df_sum[col] = df_sum[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
    print(df_sum.T.to_string(header=False))

    print("\n" + "="*110)
    print("2. NEIGHBORHOOD ECONOMICS: Why is the math working here?")
    print("="*110)

    econ_query = """
    SELECT
        neighborhood_name,
        COUNT(*) as sb79_feasible_lots,
        CAST(MEDIAN(condo_price_per_sqft) AS INT) as exit_price_per_sqft,
        CAST(MEDIAN(value_per_new_unit) AS INT) as exit_revenue_per_unit,
        CAST(MEDIAN(cpu_sb79) AS INT) as const_cost_per_unit,
        CAST(MEDIAN(acquisition_cost) AS INT) as median_acq_cost
    FROM step5_pro_forma
    WHERE neighborhood_name = 'WEST ELSDON' AND yield_sb79 > 0
    GROUP BY neighborhood_name
    """
    df_econ = con.execute(econ_query).df()
    format_dict_econ = {
        'exit_price_per_sqft': '${:,.0f}',
        'exit_revenue_per_unit': '${:,.0f}',
        'const_cost_per_unit': '${:,.0f}',
        'median_acq_cost': '${:,.0f}'
    }
    for col, fmt in format_dict_econ.items():
        if col in df_econ.columns:
            df_econ[col] = df_econ[col].apply(lambda x: fmt.format(x) if pd.notnull(x) else x)
    print(df_econ.to_string(index=False))

    print("\n" + "="*110)
    print("3. TRANSIT TRIGGERS: What transit is unlocking SB79?")
    print("="*110)

    transit_query = """
    SELECT
        CASE
            WHEN up.is_train_1320 THEN 'Train (1/4 Mi)'
            WHEN up.is_train_2640 THEN 'Train (1/2 Mi)'
            WHEN up.is_brt_1320 THEN 'BRT (1/4 Mi)'
            WHEN up.hf_bus_count >= 2 THEN 'Bus Intersection'
            ELSE 'Other'
        END as transit_trigger,
        COUNT(spf.prop_address) as parcel_count,
        SUM(spf.add_true_sb79) as sb79_bonus_units
    FROM step5_pro_forma spf
    JOIN unified_properties up ON spf.prop_address = up.prop_address
    WHERE spf.neighborhood_name = 'WEST ELSDON' AND spf.add_true_sb79 > 0
    GROUP BY 1
    ORDER BY sb79_bonus_units DESC
    """
    df_transit = con.execute(transit_query).df()
    print(df_transit.to_string(index=False))

    print("\n" + "="*110)
    print("4. THE WHALES: Financial Breakdown of Top 10 SB79 Projects (Fixed ROI)")
    print("="*110)

    # FIXED: True Construction Cost strips out the already-added acquisition cost
    whale_query = """
    SELECT
        prop_address,
        zone_class as zone,
        area_sqft,
        yield_sb79 as units,
        acquisition_cost as acq_cost,
        ((yield_sb79 * cpu_sb79) - acquisition_cost) as true_const_cost,
        (yield_sb79 * value_per_new_unit) as total_rev,
        ((yield_sb79 * value_per_new_unit) / NULLIF(yield_sb79 * cpu_sb79, 0)) as true_roi
    FROM step5_pro_forma
    WHERE neighborhood_name = 'WEST ELSDON' AND yield_sb79 > 0
    ORDER BY (yield_sb79 - yield_pritzker) DESC
    LIMIT 10
    """
    df_whales = con.execute(whale_query).df()

    format_dict_whales = {
        'area_sqft': '{:,.0f}',
        'units': '{:,.0f}',
        'acq_cost': '${:,.0f}',
        'true_const_cost': '${:,.0f}',
        'total_rev': '${:,.0f}',
        'true_roi': '{:.3f}'
    }

    for col, fmt in format_dict_whales.items():
        if col in df_whales.columns:
            df_whales[col] = df_whales[col].apply(lambda x: fmt.format(x) if pd.notnull(x) else x)

    print(df_whales.to_string(index=False))

    print("\n" + "="*110)
    print("5. THE SALES ANOMALY: What recent sales are driving the $383/sqft price?")
    print("="*110)

    # RECREATING the dynamic_condo_values logic to see the raw sales
    sales_query = """
    WITH flat_characteristics AS (
        SELECT pin, MAX(TRY_CAST(char_yrblt AS INT)) as yrblt, MAX(TRY_CAST(char_bldg_sf AS DOUBLE)) as sqft
        FROM res_characteristics WHERE CAST(class AS VARCHAR) IN ('211', '212') GROUP BY pin
    ),
    condo_chars_clean AS (
        SELECT pin, MAX(TRY_CAST(year_built AS INT)) as yrblt, MAX(NULLIF(TRY_CAST(unit_sf AS DOUBLE), 0)) as sqft
        FROM condo_characteristics GROUP BY pin
    ),
    recent_new_sales AS (
        SELECT s.pin, TRY_CAST(s.sale_price AS DOUBLE) as sale_price, s.sale_date, f.sqft, f.yrblt, 'Flat (211/212)' as prop_type
        FROM parcel_sales s JOIN flat_characteristics f ON s.pin = f.pin
        WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR AND f.yrblt >= 2018 AND f.sqft > 400 AND TRY_CAST(s.sale_price AS DOUBLE) > 50000

        UNION ALL

        SELECT s.pin, TRY_CAST(s.sale_price AS DOUBLE) as sale_price, s.sale_date, c.sqft, c.yrblt, 'Condo (299)' as prop_type
        FROM parcel_sales s JOIN condo_chars_clean c ON s.pin = c.pin
        WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR AND c.yrblt >= 2018 AND c.sqft > 400 AND TRY_CAST(s.sale_price AS DOUBLE) > 50000
    )
    SELECT
        COALESCE(pa.prop_address_full, rns.pin) as address,
        rns.prop_type,
        rns.yrblt,
        rns.sqft,
        rns.sale_price,
        ROUND(rns.sale_price / rns.sqft, 2) as ppsf,
        CAST(rns.sale_date AS DATE) as sale_date
    FROM recent_new_sales rns
    JOIN (SELECT DISTINCT pin10, neighborhood_name FROM spatial_base) sb ON SUBSTR(REPLACE(CAST(rns.pin AS VARCHAR), '-', ''), 1, 10) = sb.pin10
    LEFT JOIN (SELECT pin, ANY_VALUE(prop_address_full) as prop_address_full FROM parcel_addresses GROUP BY pin) pa ON rns.pin = pa.pin
    WHERE sb.neighborhood_name = 'WEST ELSDON'
    ORDER BY ppsf DESC
    LIMIT 20
    """
    df_sales = con.execute(sales_query).df()

    if df_sales.empty:
        print("No recent modern condo/flat sales found in West Elsdon! (Falling back to Region Median)")
    else:
        format_dict_sales = {
            'sale_price': '${:,.0f}',
            'ppsf': '${:,.2f}'
        }
        for col, fmt in format_dict_sales.items():
            if col in df_sales.columns:
                df_sales[col] = df_sales[col].apply(lambda x: fmt.format(x) if pd.notnull(x) else x)
        print(df_sales.to_string(index=False))

if __name__ == "__main__":
    run_west_elsdon_debug()
