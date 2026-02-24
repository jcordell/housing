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
      AND s.is_multisale = FALSE

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
      AND s.is_multisale = FALSE
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
        QUANTILE_CONT(sale_price / sqft, 0.80) as condo_price_per_sqft
    FROM filtered_sales
    WHERE (sale_price / sqft) BETWEEN 100 AND 1200
    GROUP BY neighborhood_name
    HAVING COUNT(*) >= 10
),
region_mapping AS (
    SELECT
        neighborhood_name,
        CASE
            WHEN neighborhood_name IN ('ROGERS PARK', 'EDGEWATER', 'UPTOWN', 'LAKE VIEW', 'LINCOLN PARK') THEN 'NORTH LAKEFONT'
            WHEN neighborhood_name IN ('NORTH CENTER', 'LINCOLN SQUARE', 'WEST RIDGE', 'ALBANY PARK', 'AVONDALE', 'IRVING PARK', 'PORTAGE PARK', 'JEFFERSON PARK', 'DUNNING', 'MONTCLARE', 'HERMOSA', 'BELMONT CRAGIN') THEN 'NORTHWEST'
            WHEN neighborhood_name IN ('LOGAN SQUARE', 'WEST TOWN', 'NEAR WEST SIDE') THEN 'WEST CORE'
            WHEN neighborhood_name IN ('LOWER WEST SIDE', 'EAST GARFIELD PARK', 'WEST GARFIELD PARK', 'NORTH LAWNDALE', 'SOUTH LAWNDALE', 'AUSTIN', 'HUMBOLDT PARK') THEN 'FAR WEST'
            WHEN neighborhood_name IN ('NEAR NORTH SIDE', 'LOOP', 'NEAR SOUTH SIDE') THEN 'CENTRAL'
            WHEN neighborhood_name IN ('HYDE PARK', 'KENWOOD', 'OAKLAND', 'WOODLAWN', 'SOUTH SHORE') THEN 'SOUTH LAKEFONT'
            ELSE 'SOUTH/SOUTHWEST'
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
),

-- TEARDOWN FLOOR PERCENTILES
recent_teardown_sales AS (
    SELECT
        SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) as pin10,
        MAX(TRY_CAST(s.sale_price AS DOUBLE)) as sale_price
    FROM parcel_sales s
    JOIN assessor_universe au ON SUBSTR(REPLACE(CAST(s.pin AS VARCHAR), '-', ''), 1, 10) = SUBSTR(LPAD(CAST(au.pin AS VARCHAR), 14, '0'), 1, 10)
    WHERE s.sale_date >= CURRENT_DATE - INTERVAL '2' YEAR
      AND TRY_CAST(s.sale_price AS DOUBLE) > 20000
      AND s.is_multisale = FALSE
      AND CAST(au.class AS VARCHAR) IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214')
    GROUP BY 1
),
teardown_with_area AS (
    SELECT
        sb.neighborhood_name,
        rs.sale_price / sb.area_sqft as price_per_sqft_land
    FROM recent_teardown_sales rs
    JOIN (SELECT pin10, MAX(neighborhood_name) as neighborhood_name, MAX(area_sqft) as area_sqft FROM spatial_base GROUP BY pin10) sb ON rs.pin10 = sb.pin10
    WHERE sb.area_sqft > 500
),
floor_neighborhood_teardown AS (
    SELECT
        neighborhood_name,
        MEDIAN(price_per_sqft_land) as med_land,
        QUANTILE_CONT(price_per_sqft_land, 0.05) as q05,
        QUANTILE_CONT(price_per_sqft_land, 0.15) as q15,
        QUANTILE_CONT(price_per_sqft_land, 0.30) as q30
    FROM teardown_with_area
    GROUP BY neighborhood_name
    HAVING COUNT(*) >= 10
),
floor_region AS (
    SELECT
        r.region,
        MEDIAN(s.price_per_sqft_land) as med_land,
        QUANTILE_CONT(s.price_per_sqft_land, 0.05) as q05,
        QUANTILE_CONT(s.price_per_sqft_land, 0.15) as q15,
        QUANTILE_CONT(s.price_per_sqft_land, 0.30) as q30
    FROM teardown_with_area s
    JOIN region_mapping r ON s.neighborhood_name = r.neighborhood_name
    GROUP BY r.region
),
floor_city AS (
    SELECT
        MEDIAN(price_per_sqft_land) as med_land,
        QUANTILE_CONT(price_per_sqft_land, 0.05) as q05,
        QUANTILE_CONT(price_per_sqft_land, 0.15) as q15,
        QUANTILE_CONT(price_per_sqft_land, 0.30) as q30
    FROM teardown_with_area
),
final_calculations AS (
    SELECT
        n.neighborhood_name,
        COALESCE(nm.condo_price_per_sqft, rm.region_median, c.city_median, {{ default_condo_price_per_sqft }}) as condo_price_per_sqft,
        COALESCE(fnt.med_land, fr.med_land, fc.med_land) as local_med_land,
        COALESCE(fnt.q05, fr.q05, fc.q05) as local_q05,
        COALESCE(fnt.q15, fr.q15, fc.q15) as local_q15,
        COALESCE(fnt.q30, fr.q30, fc.q30) as local_q30
    FROM (SELECT DISTINCT neighborhood_name FROM spatial_base) n
    LEFT JOIN neighborhood_medians nm ON n.neighborhood_name = nm.neighborhood_name
    LEFT JOIN region_mapping r ON n.neighborhood_name = r.neighborhood_name
    LEFT JOIN region_medians rm ON r.region = rm.region
    CROSS JOIN citywide c
    LEFT JOIN floor_neighborhood_teardown fnt ON n.neighborhood_name = fnt.neighborhood_name
    LEFT JOIN floor_region fr ON r.region = fr.region
    CROSS JOIN floor_city fc
)
SELECT
    neighborhood_name,
    condo_price_per_sqft,
    COALESCE(
            CASE
                WHEN local_med_land >= 150 THEN local_q05
                WHEN local_med_land >= 75 THEN local_q15
                ELSE local_q30
                END,
        {{ default_acq_floor_per_sqft }}
    ) as acq_cost_floor_per_sqft
FROM final_calculations;
