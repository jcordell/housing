CREATE OR REPLACE TABLE unified_properties AS
WITH u_agg AS (
    SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
           ANY_VALUE(CAST("class" AS VARCHAR)) as property_class,
           COUNT(pin) as tax_pin_count
    FROM assessor_universe GROUP BY 1
),
v_agg AS (SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, SUM(TRY_CAST(certified_bldg AS DOUBLE)) as bldg_value, SUM(TRY_CAST(certified_land AS DOUBLE)) as land_value FROM assessed_values GROUP BY 1),
rc_agg AS (SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, MAX(TRY_CAST(char_yrblt AS INT)) as char_yrblt, SUM(TRY_CAST(char_bldg_sf AS DOUBLE)) as char_bldg_sf FROM res_characteristics GROUP BY 1),
pa_agg AS (SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, ANY_VALUE(CAST(prop_address_full AS VARCHAR)) as prop_address FROM parcel_addresses GROUP BY 1),

assessor_joined AS (
    SELECT
        sb.pin10,
        sb.neighborhood_name as neighborhood_name,
        sb.geom_3435,
        sb.area_sqft,
        sb.zone_class,
        sb.is_train_1320, sb.is_train_2640, sb.is_brt_1320, sb.is_brt_2640, sb.is_hf_1320, sb.all_bus_count, sb.hf_bus_count,
        u.property_class as primary_prop_class,
        GREATEST(
            CAST(u.tax_pin_count AS DOUBLE),
            CASE
                WHEN u.property_class IN ('202','203','204','205','206','207','208','209','210', '234', '278') THEN 1.0
                WHEN u.property_class = '211' THEN 2.0
                WHEN u.property_class = '212' THEN 3.0
                WHEN u.property_class = '213' THEN 5.0
                WHEN u.property_class = '214' THEN 10.0
                WHEN u.property_class LIKE '3%' OR u.property_class LIKE '9%' THEN GREATEST(1.0, FLOOR(rc.char_bldg_sf / 1000.0))
                ELSE 1.0 END
        ) as existing_units,
        CASE WHEN rc.char_yrblt IS NULL OR rc.char_yrblt = 0 THEN 0 ELSE (2024 - rc.char_yrblt) END as building_age,
        COALESCE(rc.char_bldg_sf, 0.0) as existing_sqft,
        pa.prop_address,
        (COALESCE(v.bldg_value, 0.0) / CASE WHEN u.property_class LIKE '2%' OR u.property_class LIKE '3%' OR u.property_class LIKE '9%' THEN 0.10 ELSE 0.25 END) as tot_bldg_value,
        (COALESCE(v.land_value, 0.0) / CASE WHEN u.property_class LIKE '2%' OR u.property_class LIKE '3%' OR u.property_class LIKE '9%' THEN 0.10 ELSE 0.25 END) as tot_land_value
    FROM spatial_base sb
    LEFT JOIN u_agg u ON sb.pin10 = u.pin10
    LEFT JOIN v_agg v ON sb.pin10 = v.pin10
    LEFT JOIN rc_agg rc ON sb.pin10 = rc.pin10
    LEFT JOIN pa_agg pa ON sb.pin10 = pa.pin10
),

clean_sales AS (
    SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
           TRY_CAST(sale_price AS DOUBLE) as sale_price
    FROM parcel_sales
    WHERE TRY_CAST(sale_price AS DOUBLE) > 20000
),
valid_ratios AS (
    SELECT vr_aj.neighborhood_name,
           CASE
               WHEN vr_aj.primary_prop_class IN ('211', '212', '213', '214') THEN 'MULTI_FAMILY'
               WHEN vr_aj.primary_prop_class IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278') THEN 'SFH'
               WHEN vr_aj.primary_prop_class LIKE '3%' OR vr_aj.primary_prop_class LIKE '5%' THEN 'COMMERCIAL'
               ELSE 'OTHER'
           END as prop_category,
           (s.sale_price / (vr_aj.tot_bldg_value + vr_aj.tot_land_value)) as ratio
    FROM assessor_joined vr_aj
    JOIN clean_sales s ON vr_aj.pin10 = s.pin10
    WHERE (vr_aj.tot_bldg_value + vr_aj.tot_land_value) > 20000
),
bucket_medians AS (
    SELECT neighborhood_name, prop_category, MEDIAN(ratio) as bucket_multiplier
    FROM valid_ratios
    WHERE ratio BETWEEN 0.5 AND 3.5
    GROUP BY neighborhood_name, prop_category
),
neighborhood_medians AS (
    SELECT neighborhood_name, MEDIAN(ratio) as neighborhood_multiplier
    FROM valid_ratios
    WHERE ratio BETWEEN 0.5 AND 3.5
    GROUP BY neighborhood_name
),

pin_level_values AS (
    SELECT aj.*,
           COALESCE(b.bucket_multiplier, n.neighborhood_multiplier, 1.40) as market_correction_multiplier
    FROM assessor_joined aj
    LEFT JOIN neighborhood_medians n ON aj.neighborhood_name = n.neighborhood_name
    LEFT JOIN bucket_medians b ON aj.neighborhood_name = b.neighborhood_name AND
        b.prop_category = CASE
            WHEN aj.primary_prop_class IN ('211', '212', '213', '214') THEN 'MULTI_FAMILY'
            WHEN aj.primary_prop_class IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278') THEN 'SFH'
            WHEN aj.primary_prop_class LIKE '3%' OR aj.primary_prop_class LIKE '5%' THEN 'COMMERCIAL'
            ELSE 'OTHER'
        END
)

SELECT
    COALESCE(REPLACE(prop_address, ' REAR', ''), pin10) as prop_id,
    ANY_VALUE(geom_3435) as center_geom,
    ANY_VALUE(neighborhood_name) as neighborhood_name,
    ANY_VALUE(zone_class) as zone_class,
    SUM(area_sqft) as area_sqft,
    COUNT(pin10) as parcels_combined,

    BOOL_OR(is_train_1320) as is_train_1320,
    BOOL_OR(is_train_2640) as is_train_2640,
    BOOL_OR(is_brt_1320) as is_brt_1320,
    BOOL_OR(is_brt_2640) as is_brt_2640,
    BOOL_OR(is_hf_1320) as is_hf_1320,
    MAX(all_bus_count) as all_bus_count,
    MAX(hf_bus_count) as hf_bus_count,

    ARG_MAX(primary_prop_class, tot_bldg_value + tot_land_value) as primary_prop_class,
    SUM(existing_units) as existing_units,
    MAX(building_age) as building_age,
    SUM(existing_sqft) as existing_sqft,
    ANY_VALUE(prop_address) as prop_address,
    SUM(tot_bldg_value) as tot_bldg_value,
    SUM(tot_land_value) as tot_land_value,
    ARG_MAX(market_correction_multiplier, tot_bldg_value + tot_land_value) as market_correction_multiplier
FROM pin_level_values
GROUP BY COALESCE(REPLACE(prop_address, ' REAR', ''), pin10);
