CREATE OR REPLACE TABLE neighborhood_results AS
SELECT
    neighborhood_name,
    SUM(feasible_existing) as feasible_existing,
    SUM(new_pritzker) as new_pritzker,
    SUM(add_true_sb79) as add_true_sb79,
    SUM(tot_true_sb79) as tot_true_sb79,
    SUM(add_train_only) as add_train_only,
    SUM(tot_train_only) as tot_train_only,
    SUM(add_train_and_hf_bus) as add_train_and_hf_bus,
    SUM(tot_train_and_hf_bus) as tot_train_and_hf_bus,
    SUM(add_train_and_bus_combo) as add_train_and_bus_combo,
    SUM(tot_train_and_bus_combo) as tot_train_and_bus_combo,
    SUM(parcels_combined) as total_parcels,
    SUM(area_sqft) as total_area_sqft,
    SUM(parcels_mf_zoned) as parcels_mf_zoned,
    SUM(area_mf_zoned) as area_mf_zoned,
    ST_Y(ST_Centroid(ANY_VALUE(center_geom))) as label_lat,
    ST_X(ST_Centroid(ANY_VALUE(center_geom))) as label_lon
FROM step5_pro_forma
GROUP BY neighborhood_name HAVING SUM(tot_true_sb79) > 0 OR SUM(tot_train_and_bus_combo) > 0;
