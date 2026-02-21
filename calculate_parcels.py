import duckdb
import pandas as pd
import os
from financial_model import get_financial_filter_ctes

DB_FILE = "sb79_housing.duckdb"

def prep_zillow_rents():
    if not os.path.exists('zillow_rent.csv'):
        return False

    df = pd.read_csv('zillow_rent.csv')
    df_chi = df[df['City'] == 'Chicago'].copy()
    df_chi['neighborhood_name'] = df_chi['RegionName'].str.upper().str.replace('LAKEVIEW', 'LAKE VIEW')

    date_cols = [c for c in df.columns if c.startswith('20')]
    latest_month = date_cols[-1]

    df_chi[latest_month] = pd.to_numeric(df_chi[latest_month].astype(str).str.replace(',', ''), errors='coerce')

    df_clean = df_chi[['neighborhood_name', latest_month]].rename(columns={latest_month: 'monthly_rent'})
    df_clean.to_csv('current_rents.csv', index=False)
    return True

def run_parcel_calculations(full_recalculate=True):
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    has_rents = prep_zillow_rents()

    if full_recalculate:
        print("Running Heavy Spatial Assembly with Zillow Data (This takes a few minutes)...")

        if has_rents:
            con.execute("CREATE OR REPLACE TABLE neighborhood_rents AS SELECT * FROM read_csv_auto('current_rents.csv')")
        else:
            con.execute("CREATE OR REPLACE TABLE neighborhood_rents (neighborhood_name VARCHAR, monthly_rent DOUBLE)")

        con.execute("""
            CREATE OR REPLACE TABLE parcel_base_unfiltered AS
            WITH
            target_zones AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, zone_class FROM zoning WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'),
            projected_transit AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM transit_stops),
            projected_bus_all AS (SELECT CAST(route AS VARCHAR) as route, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM bus_routes),
            projected_bus_hf AS (SELECT geom_3435 FROM projected_bus_all WHERE route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')),
            projected_bus_brt AS (SELECT geom_3435 FROM projected_bus_all WHERE route = 'J14'),
            processed_parcels AS (SELECT pin10, SUBSTR(pin10, 1, 7) as block_id, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM parcels WHERE geom IS NOT NULL),

            combined_assessor AS (
                SELECT
                    u.pin10, u.property_class, u.estimated_existing_units,
                    COALESCE(TRY_CAST(v.bldg_value AS DOUBLE), 0.0) as bldg_value,
                    COALESCE(TRY_CAST(v.land_value AS DOUBLE), 0.0) as land_value,
                    CASE WHEN u.property_class LIKE '2%' THEN 0.10 ELSE 0.25 END as assessment_level
                FROM (
                    SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, CAST("class" AS VARCHAR) as property_class,
                        CASE
                            WHEN CAST("class" AS VARCHAR) IN ('202','203','204','205','206','207','208','209','210', '234', '278') THEN 1.0
                            WHEN CAST("class" AS VARCHAR) = '211' THEN 2.0
                            WHEN CAST("class" AS VARCHAR) = '212' THEN 3.0
                            WHEN CAST("class" AS VARCHAR) = '213' THEN 5.0
                            WHEN CAST("class" AS VARCHAR) = '214' THEN 10.0
                            ELSE 1.0 END as estimated_existing_units
                    FROM assessor_universe
                ) u
                LEFT JOIN (
                    SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10, TRY_CAST(certified_bldg AS DOUBLE) as bldg_value, TRY_CAST(certified_land AS DOUBLE) as land_value
                    FROM assessed_values
                ) v ON u.pin10 = v.pin10
            ),

            market_values AS (
                SELECT pin10, property_class, estimated_existing_units,
                    (bldg_value / NULLIF(assessment_level, 0)) as bldg_market_value,
                    (land_value / NULLIF(assessment_level, 0)) as land_market_value
                FROM combined_assessor
            ),

            parcel_zone_join AS (SELECT p.pin10, p.block_id, p.geom_3435, ST_Area(p.geom_3435) as area_sqft, z.zone_class FROM processed_parcels p JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)),

            eligible_parcels AS (
                SELECT pz.pin10, ANY_VALUE(pz.block_id) as block_id, ANY_VALUE(pz.geom_3435) as geom_3435, ANY_VALUE(pz.area_sqft) as area_sqft, ANY_VALUE(pz.zone_class) as zone_class,
                ANY_VALUE(m.property_class) as property_class, SUM(m.estimated_existing_units) as existing_units,
                SUM(m.bldg_market_value) as bldg_market_value, SUM(m.land_market_value) as land_market_value
                FROM parcel_zone_join pz
                LEFT JOIN market_values m ON pz.pin10 = m.pin10
                GROUP BY pz.pin10
            ),

            parcels_with_neighborhoods AS (
                SELECT ep.*, UPPER(n.community) as neighborhood_name
                FROM eligible_parcels ep
                LEFT JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(ST_Transform(ep.geom_3435, 'EPSG:3435', 'EPSG:4326', true), n.geom)
            ),

            assembled_lots AS (
                SELECT block_id, zone_class, ANY_VALUE(neighborhood_name) as neighborhood_name, ST_Union_Agg(geom_3435) as assembled_geom, ST_Transform(ST_Centroid(ST_Union_Agg(geom_3435)), 'EPSG:3435', 'EPSG:4326', true) as center_geom,
                SUM(area_sqft) as assembled_area_sqft, COUNT(pin10) as parcels_combined,
                SUM(existing_units) as tot_existing_units, ANY_VALUE(property_class) as primary_prop_class,
                SUM(bldg_market_value) as tot_bldg_value, SUM(land_market_value) as tot_land_value
                FROM parcels_with_neighborhoods GROUP BY block_id, zone_class
            ),

            parcel_bus_counts AS (
                SELECT a.block_id, a.zone_class, COUNT(DISTINCT b_all.route) as all_bus_count, COUNT(DISTINCT CASE WHEN b_all.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b_all.route END) as hf_bus_count
                FROM assembled_lots a JOIN projected_bus_all b_all ON ST_Distance(a.assembled_geom, b_all.geom_3435) <= 1320 GROUP BY a.block_id, a.zone_class
            ),

            parcel_distances AS (
                SELECT a.block_id, a.center_geom, a.neighborhood_name, a.assembled_area_sqft as area_sqft, a.parcels_combined, a.zone_class, a.tot_existing_units, a.primary_prop_class, a.tot_bldg_value, a.tot_land_value,
                COALESCE(pbc.all_bus_count, 0) as all_bus_count, COALESCE(pbc.hf_bus_count, 0) as hf_bus_count, MIN(ST_Distance(a.assembled_geom, t.geom_3435)) as min_dist_train, MIN(ST_Distance(a.assembled_geom, b_brt.geom_3435)) as min_dist_brt, MIN(ST_Distance(a.assembled_geom, b_hf.geom_3435)) as min_dist_hf_bus
                FROM assembled_lots a LEFT JOIN projected_transit t ON ST_Distance(a.assembled_geom, t.geom_3435) <= 2640 LEFT JOIN projected_bus_brt b_brt ON ST_Distance(a.assembled_geom, b_brt.geom_3435) <= 2640 LEFT JOIN projected_bus_hf b_hf ON ST_Distance(a.assembled_geom, b_hf.geom_3435) <= 1320 LEFT JOIN parcel_bus_counts pbc ON a.block_id = pbc.block_id AND a.zone_class = pbc.zone_class
                GROUP BY a.block_id, a.center_geom, a.neighborhood_name, a.assembled_area_sqft, a.parcels_combined, a.zone_class, a.tot_existing_units, a.primary_prop_class, a.tot_bldg_value, a.tot_land_value, pbc.all_bus_count, pbc.hf_bus_count
            )

            SELECT pd.center_geom, pd.neighborhood_name, pd.area_sqft, pd.zone_class, pd.parcels_combined, pd.tot_existing_units, pd.primary_prop_class, pd.tot_bldg_value, pd.tot_land_value,
                COALESCE(TRY_CAST(r.monthly_rent AS DOUBLE), 1800.0) as local_rent,
                GREATEST(pd.parcels_combined, CASE WHEN pd.zone_class LIKE 'RS-1%' OR pd.zone_class LIKE 'RS-2%' THEN FLOOR(pd.area_sqft / 5000) WHEN pd.zone_class LIKE 'RS-3%' THEN FLOOR(pd.area_sqft / 2500) WHEN pd.zone_class LIKE 'RT-3.5%' THEN FLOOR(pd.area_sqft / 1250) WHEN pd.zone_class LIKE 'RT-4%' THEN FLOOR(pd.area_sqft / 1000) WHEN pd.zone_class LIKE 'RM-4.5%' OR pd.zone_class LIKE 'RM-5%' THEN FLOOR(pd.area_sqft / 400) WHEN pd.zone_class LIKE 'RM-6%' OR pd.zone_class LIKE 'RM-6.5%' THEN FLOOR(pd.area_sqft / 200) WHEN pd.zone_class LIKE '%-1' THEN FLOOR(pd.area_sqft / 1000) WHEN pd.zone_class LIKE '%-2' OR pd.zone_class LIKE '%-3' THEN FLOOR(pd.area_sqft / 400) WHEN pd.zone_class LIKE '%-5' OR pd.zone_class LIKE '%-6' THEN FLOOR(pd.area_sqft / 200) ELSE FLOOR(pd.area_sqft / 1000) END) as current_capacity,
                CASE WHEN pd.zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN CASE WHEN (pd.area_sqft / pd.parcels_combined) < 2500 THEN 1 * pd.parcels_combined WHEN (pd.area_sqft / pd.parcels_combined) < 5000 THEN 4 * pd.parcels_combined WHEN (pd.area_sqft / pd.parcels_combined) < 7500 THEN 6 * pd.parcels_combined ELSE 8 * pd.parcels_combined END ELSE 0 END as pritzker_capacity,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.min_dist_train <= 1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) WHEN pd.min_dist_train <= 2640 OR pd.min_dist_brt <= 1320 OR pd.hf_bus_count >= 2 THEN FLOOR((pd.area_sqft / 43560.0) * 100) WHEN pd.min_dist_brt <= 2640 THEN FLOOR((pd.area_sqft / 43560.0) * 80) ELSE 0 END as cap_true_sb79,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.min_dist_train <= 1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) WHEN pd.min_dist_train <= 2640 THEN FLOOR((pd.area_sqft / 43560.0) * 100) ELSE 0 END as cap_train_only,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.min_dist_train <= 2640 AND pd.min_dist_hf_bus <= 1320 THEN CASE WHEN pd.min_dist_train <= 1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) ELSE FLOOR((pd.area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_hf_bus,
                CASE WHEN pd.area_sqft < 5000 THEN 0 WHEN pd.min_dist_train <= 2640 AND (pd.min_dist_hf_bus <= 1320 OR pd.all_bus_count >= 2) THEN CASE WHEN pd.min_dist_train <= 1320 THEN FLOOR((pd.area_sqft / 43560.0) * 120) ELSE FLOOR((pd.area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_bus_combo
            FROM parcel_distances pd
            LEFT JOIN neighborhood_rents r ON pd.neighborhood_name = r.neighborhood_name;
        """)
        print("✅ Spatial mapping complete and saved to un-filtered cache.")

    print("Applying Real Estate Pro Forma Feasibility Filters...")

    # DYNAMICALLY INJECT YOUR FINANCIAL LOGIC
    financial_ctes = get_financial_filter_ctes("parcel_base_unfiltered")

    con.execute(f"""
        CREATE OR REPLACE TABLE neighborhood_results AS
        WITH {financial_ctes}
        SELECT
            neighborhood_name,
            SUM(fp.feasible_existing) as feasible_existing,
            SUM(fp.new_pritzker) as new_pritzker,
            SUM(fp.add_true_sb79) as add_true_sb79,
            SUM(fp.tot_true_sb79) as tot_true_sb79,
            SUM(fp.add_train_only) as add_train_only,
            SUM(fp.tot_train_only) as tot_train_only,
            SUM(fp.add_train_and_hf_bus) as add_train_and_hf_bus,
            SUM(fp.tot_train_and_hf_bus) as tot_train_and_hf_bus,
            SUM(fp.add_train_and_bus_combo) as add_train_and_bus_combo,
            SUM(fp.tot_train_and_bus_combo) as tot_train_and_bus_combo,

            SUM(fp.parcels_combined) as total_parcels,
            SUM(fp.area_sqft) as total_area_sqft,
            SUM(fp.parcels_mf_zoned) as parcels_mf_zoned,
            SUM(fp.area_mf_zoned) as area_mf_zoned,

            ST_Y(ST_Centroid(ANY_VALUE(fp.center_geom))) as label_lat,
            ST_X(ST_Centroid(ANY_VALUE(fp.center_geom))) as label_lon
        FROM filtered_parcels fp
        GROUP BY neighborhood_name HAVING SUM(fp.tot_true_sb79) > 0 OR SUM(fp.tot_train_and_bus_combo) > 0
    """)
    con.close()
    print("✅ Financial Pro Forma filters applied. Neighborhood results updated.")
