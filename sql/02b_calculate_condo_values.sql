CREATE OR REPLACE TABLE dynamic_condo_values AS
WITH new_build_sales AS (
    SELECT
        SUBSTR(LPAD(CAST(s.pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
        TRY_CAST(s.sale_price AS DOUBLE) as sale_price,
        TRY_CAST(c.char_bldg_sf AS DOUBLE) as sqft,
        TRY_CAST(c.char_yrblt AS INT) as yrblt
    FROM parcel_sales s
    JOIN res_characteristics c ON s.pin = c.pin
    WHERE TRY_CAST(s.sale_price AS DOUBLE) > 50000
      AND TRY_CAST(c.char_yrblt AS INT) >= 2018
      AND TRY_CAST(c.char_bldg_sf AS DOUBLE) > 400
      AND CAST(s.class AS VARCHAR) IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '234', '278')
),
sales_with_neighborhood AS (
    SELECT
        sb.neighborhood_name,
        s.sale_price / s.sqft as price_per_sqft
    FROM new_build_sales s
    JOIN (SELECT DISTINCT pin10, neighborhood_name FROM spatial_base) sb ON s.pin10 = sb.pin10
),
neighborhood_medians AS (
    SELECT
        neighborhood_name,
        MEDIAN(price_per_sqft) as condo_price_per_sqft
    FROM sales_with_neighborhood
    WHERE price_per_sqft BETWEEN 100 AND 1200
    GROUP BY neighborhood_name
),
citywide AS (
    SELECT MEDIAN(price_per_sqft) as city_median
    FROM sales_with_neighborhood
    WHERE price_per_sqft BETWEEN 100 AND 1200
)
SELECT
    n.neighborhood_name,
    COALESCE(nm.condo_price_per_sqft, c.city_median, {{ default_condo_price_per_sqft }}) as condo_price_per_sqft
FROM (SELECT DISTINCT neighborhood_name FROM spatial_base) n
         LEFT JOIN neighborhood_medians nm ON n.neighborhood_name = nm.neighborhood_name
         CROSS JOIN citywide c;
