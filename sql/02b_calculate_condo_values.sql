CREATE OR REPLACE TABLE dynamic_condo_values AS
WITH flat_characteristics AS (
    SELECT
        pin,
        MAX(TRY_CAST(char_yrblt AS INT)) as yrblt,
        MAX(TRY_CAST(char_bldg_sf AS DOUBLE)) as sqft
    FROM res_characteristics
    WHERE CAST(class AS VARCHAR) IN ('211', '212')
    GROUP BY pin
),
condo_chars_clean AS (
    SELECT
        pin,
        MAX(TRY_CAST(year_built AS INT)) as yrblt,
        MAX(NULLIF(TRY_CAST(unit_sf AS DOUBLE), 0)) as sqft
    FROM condo_characteristics
    GROUP BY pin
),
recent_new_sales AS (
    SELECT
        SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) as pin10,
        TRY_CAST(s.sale_price AS DOUBLE) as sale_price,
        f.sqft
    FROM parcel_sales s
    JOIN flat_characteristics f ON s.pin = f.pin
    WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
      AND f.yrblt >= 2018
      AND f.sqft > 400
      AND TRY_CAST(s.sale_price AS DOUBLE) > 50000
      AND s.is_multisale = FALSE -- FIX 1: Filter out bulk/multi-parcel sales

    UNION ALL

    SELECT
        SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) as pin10,
        TRY_CAST(s.sale_price AS DOUBLE) as sale_price,
        c.sqft
    FROM parcel_sales s
    JOIN condo_chars_clean c ON s.pin = c.pin
    WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
      AND c.yrblt >= 2018
      AND c.sqft > 400
      AND TRY_CAST(s.sale_price AS DOUBLE) > 50000
      AND s.is_multisale = FALSE -- FIX 1: Filter out bulk/multi-parcel sales
),
filtered_sales AS (
    SELECT
        rns.sale_price,
        rns.sqft,
        sb.neighborhood_name
    FROM recent_new_sales rns
    JOIN (SELECT DISTINCT pin10, neighborhood_name FROM spatial_base) sb ON rns.pin10 = sb.pin10
),
neighborhood_medians AS (
    SELECT
        neighborhood_name,
        MEDIAN(sale_price / sqft) as condo_price_per_sqft
    FROM filtered_sales
    WHERE (sale_price / sqft) BETWEEN 100 AND 1200
    GROUP BY neighborhood_name
    HAVING COUNT(*) >= 10 -- FIX 2: Require 10 sales to trust neighborhood median
),
region_mapping AS (
    SELECT
        neighborhood_name,
        CASE
            WHEN neighborhood_name IN ('ROGERS PARK', 'EDGEWATER', 'UPTOWN', 'LAKE VIEW', 'LINCOLN PARK', 'NORTH CENTER', 'LINCOLN SQUARE', 'WEST RIDGE', 'ALBANY PARK') THEN 'NORTH'
            WHEN neighborhood_name IN ('LOGAN SQUARE', 'WEST TOWN', 'NEAR WEST SIDE', 'LOWER WEST SIDE', 'EAST GARFIELD PARK', 'WEST GARFIELD PARK', 'NORTH LAWNDALE', 'SOUTH LAWNDALE', 'AUSTIN', 'HUMBOLDT PARK', 'BELMONT CRAGIN', 'HERMOSA', 'AVONDALE', 'IRVING PARK', 'PORTAGE PARK', 'JEFFERSON PARK', 'DUNNING', 'MONTCLARE') THEN 'WEST'
            WHEN neighborhood_name IN ('NEAR NORTH SIDE', 'LOOP', 'NEAR SOUTH SIDE') THEN 'CENTRAL'
            ELSE 'SOUTH'
        END as region
    FROM (SELECT DISTINCT neighborhood_name FROM spatial_base)
),
sales_with_region AS (
    SELECT
        (fs.sale_price / fs.sqft) as price_per_sqft,
        r.region
    FROM filtered_sales fs
    JOIN region_mapping r ON fs.neighborhood_name = r.neighborhood_name
),
region_medians AS (
    SELECT
        region,
        MEDIAN(price_per_sqft) as region_median
    FROM sales_with_region
    WHERE price_per_sqft BETWEEN 100 AND 1200
    GROUP BY region
),
citywide AS (
    SELECT MEDIAN(sale_price / sqft) as city_median
    FROM filtered_sales
    WHERE (sale_price / sqft) BETWEEN 100 AND 1200
)
SELECT
    n.neighborhood_name,
    COALESCE(nm.condo_price_per_sqft, rm.region_median, c.city_median, {{ default_condo_price_per_sqft }}) as condo_price_per_sqft
FROM (SELECT DISTINCT neighborhood_name FROM spatial_base) n
         LEFT JOIN neighborhood_medians nm ON n.neighborhood_name = nm.neighborhood_name
         LEFT JOIN region_mapping r ON n.neighborhood_name = r.neighborhood_name
         LEFT JOIN region_medians rm ON r.region = rm.region
         CROSS JOIN citywide c;
