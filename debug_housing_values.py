import duckdb
import pandas as pd

def run_redevelopment_audit():
    con = duckdb.connect('data/sb79_housing.duckdb')

    print("\n" + "="*80)
    print("AUDIT: PPSF BY NEIGHBORHOOD (MOST TO LEAST EXPENSIVE)")
    print("="*80)

    # Leaderboard of neighborhood values
    leaderboard_query = """
    SELECT
        neighborhood_name,
        ROUND(condo_price_per_sqft, 2) as ppsf,
        (SELECT COUNT(*) FROM parcel_sales s
         JOIN spatial_base sb ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = sb.pin10
         WHERE sb.neighborhood_name = d.neighborhood_name
         AND s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR) as recent_sales_volume
    FROM dynamic_condo_values d
    ORDER BY condo_price_per_sqft DESC
    """
    print(con.execute(leaderboard_query).df().to_string(index=False))

    print("\n" + "="*80)
    print("FEASIBILITY KILLER: CONDO (299) VS FLAT (211/212) PRICE GAP")
    print("="*80)

    # Check if Condos are inflating the acquisition floor
    gap_query = """
    WITH raw_stats AS (
        SELECT
            sb.neighborhood_name,
            s.class,
            AVG(s.sale_price / COALESCE(cc.unit_sf, u.char_bldg_sf, 1200)) as avg_ppsf
        FROM parcel_sales s
        LEFT JOIN condo_characteristics cc ON s.pin = cc.pin
        LEFT JOIN res_characteristics u ON s.pin = u.pin
        JOIN spatial_base sb ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = sb.pin10
        WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
          AND s.class IN ('211', '212', '299')
        GROUP BY 1, 2
    )
    SELECT
        neighborhood_name,
        ROUND(MAX(CASE WHEN class = '299' THEN avg_ppsf END), 2) as condo_ppsf,
        ROUND(MAX(CASE WHEN class IN ('211', '212') THEN avg_ppsf END), 2) as flat_ppsf,
        ROUND(MAX(CASE WHEN class = '299' THEN avg_ppsf END) /
              NULLIF(MAX(CASE WHEN class IN ('211', '212') THEN avg_ppsf END), 0), 2) as inflation_factor
    FROM raw_stats
    GROUP BY 1
    HAVING inflation_factor IS NOT NULL
    ORDER BY inflation_factor DESC
    LIMIT 15
    """
    print(con.execute(gap_query).df().to_string(index=False))

if __name__ == "__main__":
    run_redevelopment_audit()
