import duckdb
import pandas as pd

def run_redevelopment_audit():
    con = duckdb.connect('data/sb79_housing.duckdb')

    print("\n" + "="*80)
    print("AUDIT: PPSF BY NEIGHBORHOOD (MOST TO LEAST EXPENSIVE)")
    print("="*80)

    # Leaderboard of neighborhood values - updated to use clean data
    leaderboard_query = """
    WITH flat_chars AS (
        SELECT pin, MAX(TRY_CAST(char_bldg_sf AS DOUBLE)) as sqft FROM res_characteristics WHERE class IN ('211','212') GROUP BY pin
    ),
    condo_chars AS (
        SELECT pin, MAX(NULLIF(TRY_CAST(unit_sf AS DOUBLE), 0)) as sqft FROM condo_characteristics GROUP BY pin
    )
    SELECT
        d.neighborhood_name,
        ROUND(d.condo_price_per_sqft, 2) as ppsf,
        (SELECT COUNT(*)
         FROM parcel_sales s
         LEFT JOIN flat_chars f ON s.pin = f.pin
         LEFT JOIN condo_chars c ON s.pin = c.pin
         JOIN spatial_base sb ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = sb.pin10
         WHERE sb.neighborhood_name = d.neighborhood_name
           AND s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
           AND s.class IN ('211', '212', '299')
           AND (f.sqft > 400 OR c.sqft > 400)
        ) as clean_recent_sales_volume
    FROM dynamic_condo_values d
    ORDER BY d.condo_price_per_sqft DESC
    LIMIT 20
    """
    print(con.execute(leaderboard_query).df().to_string(index=False))

    print("\n" + "="*80)
    print("FEASIBILITY KILLER: CONDO (299) VS FLAT (211/212) PRICE GAP")
    print("="*80)

    # Check if Condos are inflating the acquisition floor using flattened, clean data
    gap_query = """
    WITH flat_chars AS (
        SELECT pin, MAX(TRY_CAST(char_bldg_sf AS DOUBLE)) as sqft FROM res_characteristics WHERE class IN ('211','212') GROUP BY pin
    ),
    condo_chars AS (
        SELECT pin, MAX(NULLIF(TRY_CAST(unit_sf AS DOUBLE), 0)) as sqft FROM condo_characteristics GROUP BY pin
    ),
    raw_stats AS (
        SELECT
            sb.neighborhood_name,
            s.class,
            CASE WHEN s.class = '299' THEN (s.sale_price / c.sqft)
                 WHEN s.class IN ('211', '212') THEN (s.sale_price / f.sqft)
            END as ppsf
        FROM parcel_sales s
        LEFT JOIN flat_chars f ON s.pin = f.pin
        LEFT JOIN condo_chars c ON s.pin = c.pin
        JOIN spatial_base sb ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = sb.pin10
        WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
          AND s.class IN ('211', '212', '299')
          AND TRY_CAST(s.sale_price AS DOUBLE) > 50000
          AND (f.sqft > 400 OR c.sqft > 400)
    )
    SELECT
        neighborhood_name,
        ROUND(AVG(CASE WHEN class = '299' THEN ppsf END), 2) as avg_condo_ppsf,
        ROUND(AVG(CASE WHEN class IN ('211', '212') THEN ppsf END), 2) as avg_flat_ppsf,
        ROUND(AVG(CASE WHEN class = '299' THEN ppsf END) /
              NULLIF(AVG(CASE WHEN class IN ('211', '212') THEN ppsf END), 0), 2) as inflation_factor
    FROM raw_stats
    GROUP BY 1
    HAVING inflation_factor IS NOT NULL
    ORDER BY inflation_factor DESC
    LIMIT 15
    """
    print(con.execute(gap_query).df().to_string(index=False))

if __name__ == "__main__":
    run_redevelopment_audit()
