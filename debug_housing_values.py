import duckdb
import pandas as pd

def run_condo_value_debug():
    con = duckdb.connect('data/sb79_housing.duckdb')

    print("\n" + "="*80)
    print("NEW BUILD SALES: Condos & Flats ONLY (Recent Sales)")
    print("="*80)

    debug_query = """
    WITH base_parcel_chars AS (
        -- Fetch the Year Built from the Base PIN so condos don't drop out
        SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
               MAX(TRY_CAST(char_yrblt AS INT)) as base_yrblt
        FROM res_characteristics
        GROUP BY 1
    ),
    raw_sales AS (
        SELECT
            SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) as pin10,
            TRY_CAST(s.sale_price AS DOUBLE) as sale_price,
            CAST(s.class AS VARCHAR) as prop_class,
            -- Inject 1200 sqft for condos
            CASE WHEN CAST(s.class AS VARCHAR) = '299' THEN 1200.0 ELSE TRY_CAST(c.char_bldg_sf AS DOUBLE) END as sqft,
            -- Use the unit's year built, or fallback to the Base PIN's year built
            COALESCE(TRY_CAST(c.char_yrblt AS INT), bc.base_yrblt) as yrblt,
            TRY_CAST(s.sale_date AS DATE) as sale_date
        FROM parcel_sales s
        LEFT JOIN res_characteristics c ON s.pin = c.pin
        LEFT JOIN base_parcel_chars bc ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = bc.pin10
        WHERE TRY_CAST(s.sale_price AS DOUBLE) > 50000
          -- RESTRICT to Condos and Multi-Family. No Single Family Homes.
          AND CAST(s.class AS VARCHAR) IN ('211', '212', '213', '214', '299')
    )
    SELECT
        sb.neighborhood_name,
        CASE WHEN rs.prop_class = '299' THEN 'Condominium (299)' ELSE 'Multi-Family Flat' END as property_type,
        COUNT(*) as sales_count,
        CAST(MEDIAN(rs.sale_price / rs.sqft) AS INT) as median_price_per_sqft
    FROM raw_sales rs
    JOIN (SELECT DISTINCT pin10, neighborhood_name FROM spatial_base) sb ON rs.pin10 = sb.pin10
    WHERE rs.yrblt >= 2015
      AND EXTRACT(YEAR FROM rs.sale_date) >= 2021  -- Only look at sales from the last few years
      AND (rs.sale_price / rs.sqft) BETWEEN 100 AND 1200
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    df_types = con.execute(debug_query).df()

    # Check the key neighborhoods to verify Condos finally populate
    sample_hoods = ['LINCOLN PARK', 'LAKE VIEW', 'ENGLEWOOD', 'WOODLAWN']
    print(df_types[df_types['neighborhood_name'].isin(sample_hoods)].to_string(index=False))

if __name__ == "__main__":
    run_condo_value_debug()
