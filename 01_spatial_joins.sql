CREATE OR REPLACE TABLE spatial_base AS
WITH nbhds AS (
    SELECT geom, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, UPPER(community) as neighborhood_name
    FROM ST_Read('data/neighborhoods.geojson')
    {% if is_sandbox %}
    WHERE UPPER(community) IN ('LINCOLN PARK', 'LAKE VIEW', 'ASHBURN', 'AUSTIN')
    {% endif %}
),
step1_parcels AS (
    SELECT p.pin10, ST_Transform(p.geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, n.neighborhood_name
    FROM parcels p
    JOIN nbhds n ON ST_Intersects(p.geom, n.geom)
    WHERE p.geom IS NOT NULL
),
target_zones AS (
    SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, zone_class
    FROM zoning WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'
),
base_parcels AS (
    SELECT p.pin10, p.geom_3435, p.neighborhood_name, ST_Area(p.geom_3435) as area_sqft, z.zone_class
    FROM step1_parcels p
    JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)
),
projected_transit AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM transit_stops),
projected_bus_all AS (SELECT CAST(route AS VARCHAR) as route, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM bus_routes),
projected_bus_hf AS (SELECT geom_3435 FROM projected_bus_all WHERE route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')),
projected_bus_brt AS (SELECT geom_3435 FROM projected_bus_all WHERE route = 'J14'),

train_1320 AS (SELECT DISTINCT ep.pin10 FROM base_parcels ep JOIN projected_transit t ON ST_Intersects(ep.geom_3435, ST_Buffer(t.geom_3435, 1320))),
train_2640 AS (SELECT DISTINCT ep.pin10 FROM base_parcels ep JOIN projected_transit t ON ST_Intersects(ep.geom_3435, ST_Buffer(t.geom_3435, 2640))),
brt_1320 AS (SELECT DISTINCT ep.pin10 FROM base_parcels ep JOIN projected_bus_brt b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320))),
brt_2640 AS (SELECT DISTINCT ep.pin10 FROM base_parcels ep JOIN projected_bus_brt b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 2640))),
hf_1320 AS (SELECT DISTINCT ep.pin10 FROM base_parcels ep JOIN projected_bus_hf b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320))),

bus_counts AS (
    SELECT ep.pin10, COUNT(DISTINCT b.route) as all_bus_count, COUNT(DISTINCT CASE WHEN b.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b.route END) as hf_bus_count
    FROM base_parcels ep JOIN projected_bus_all b ON ST_Intersects(ep.geom_3435, ST_Buffer(b.geom_3435, 1320)) GROUP BY ep.pin10
)

SELECT ep.*,
       CASE WHEN t13.pin10 IS NOT NULL THEN true ELSE false END as is_train_1320,
       CASE WHEN t26.pin10 IS NOT NULL THEN true ELSE false END as is_train_2640,
       CASE WHEN b13.pin10 IS NOT NULL THEN true ELSE false END as is_brt_1320,
       CASE WHEN b26.pin10 IS NOT NULL THEN true ELSE false END as is_brt_2640,
       CASE WHEN h13.pin10 IS NOT NULL THEN true ELSE false END as is_hf_1320,
       COALESCE(bc.all_bus_count, 0) as all_bus_count,
       COALESCE(bc.hf_bus_count, 0) as hf_bus_count
FROM base_parcels ep
         LEFT JOIN train_1320 t13 ON ep.pin10 = t13.pin10
         LEFT JOIN train_2640 t26 ON ep.pin10 = t26.pin10
         LEFT JOIN brt_1320 b13 ON ep.pin10 = b13.pin10
         LEFT JOIN brt_2640 b26 ON ep.pin10 = b26.pin10
         LEFT JOIN hf_1320 h13 ON ep.pin10 = h13.pin10
         LEFT JOIN bus_counts bc ON ep.pin10 = bc.pin10;
