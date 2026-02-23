import duckdb
import pandas as pd

def run_feasibility_debug():
    con = duckdb.connect('data/sb79_housing.duckdb')

    print("\n" + "="*90)
    print("REDEVELOPMENT SPREAD: ACQUISITION (FLATS) VS EXIT (CONDOS)")
    print("="*90)

    # This query compares what you buy (Flats) vs what you sell (New Condos)
    spread_query = """
    WITH neighborhood_stats AS (
        SELECT 
            sb.neighborhood_name,
            MEDIAN(CASE WHEN s.class IN ('211', '212') THEN (s.sale_price / COALESCE(u.char_bldg_sf, 1200)) END) as acq_ppsf,
            MEDIAN(CASE WHEN s.class = '299' AND TRY_CAST(cc.year_built AS INT) >= 2018 THEN (s.sale_price / COALESCE(cc.unit_sf, 1200)) END) as exit_ppsf,
            COUNT(CASE WHEN s.class IN ('211', '212') THEN 1 END) as flat_sales,
            COUNT(CASE WHEN s.class = '299' THEN 1 END) as condo_sales
        FROM parcel_sales s
        LEFT JOIN condo_characteristics cc ON s.pin = cc.pin
        LEFT JOIN res_characteristics u ON s.pin = u.pin
        JOIN spatial_base sb ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = sb.pin10
        WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
        GROUP BY 1
    )
    SELECT 
        neighborhood_name,
        ROUND(acq_ppsf, 2) as purchase_price,
        ROUND(exit_ppsf, 2) as sell_price,
        ROUND(exit_ppsf - acq_ppsf, 2) as profit_spread,
        flat_sales,
        condo_sales
    FROM neighborhood_stats
    WHERE acq_ppsf IS NOT NULL AND exit_ppsf IS NOT NULL
    ORDER BY profit_spread DESC
    LIMIT 20
    """
    df = con.execute(spread_query).df()
    print(df.to_string(index=False))

    print("\n" + "="*90)
    print("TOP 10 FEASIBILITY KILLERS (High Acq Cost, Low Spread)")
    print("="*90)
    print(df.sort_values('profit_spread').head(10).to_string(index=False))

if __name__ == "__main__":
    run_feasibility_debug()
