import duckdb
import pandas as pd
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def analyze_new_builds():
    config = load_config()
    db_file = config['database']['file_name']

    con = duckdb.connect(db_file)

    query = """
    WITH spatial_mapping AS (
        -- Map 10-digit PINs to neighborhoods using the existing spatial base
        SELECT DISTINCT pin10, neighborhood_name
        FROM spatial_base
    ),
    sfh_units AS (
        SELECT
            sm.neighborhood_name,
            COUNT(DISTINCT rc.pin) AS sfhs
        FROM res_characteristics rc
        JOIN spatial_mapping sm
          ON SUBSTR(REPLACE(CAST(rc.pin AS VARCHAR), '-', ''), 1, 10) = sm.pin10
        WHERE TRY_CAST(rc.char_yrblt AS INT) >= 2018
          AND CAST(rc.class AS VARCHAR) IN ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278')
        GROUP BY sm.neighborhood_name
    ),
    condo_units AS (
        SELECT
            sm.neighborhood_name,
            COUNT(DISTINCT cc.pin) AS condos
        FROM condo_characteristics cc
        JOIN spatial_mapping sm
          ON SUBSTR(REPLACE(CAST(cc.pin AS VARCHAR), '-', ''), 1, 10) = sm.pin10
        WHERE TRY_CAST(cc.year_built AS INT) >= 2018
        GROUP BY sm.neighborhood_name
    ),
    mf_base AS (
        -- Get the base property class and total sqft for multi-family buildings
        SELECT
            SUBSTR(REPLACE(CAST(v.pin AS VARCHAR), '-', ''), 1, 10) AS pin10,
            ANY_VALUE(CAST(v.class AS VARCHAR)) AS property_class,
            SUM(TRY_CAST(rc.char_bldg_sf AS DOUBLE)) AS char_bldg_sf,
            COUNT(v.pin) AS tax_pin_count
        FROM assessed_values v
        JOIN res_characteristics rc ON v.pin = rc.pin
        WHERE TRY_CAST(rc.char_yrblt AS INT) >= 2018
          AND (
              CAST(v.class AS VARCHAR) IN ('211', '212', '213', '214')
              OR CAST(v.class AS VARCHAR) LIKE '3%'
              OR CAST(v.class AS VARCHAR) LIKE '9%'
          )
        GROUP BY 1
    ),
    mf_units_calc AS (
        -- Apply the existing pro-forma logic to estimate rental units
        SELECT
            sm.neighborhood_name,
            SUM(
                GREATEST(
                    CAST(mf.tax_pin_count AS DOUBLE),
                    CASE
                        WHEN mf.property_class = '211' THEN 2.0
                        WHEN mf.property_class = '212' THEN 3.0
                        WHEN mf.property_class = '213' THEN 5.0
                        WHEN mf.property_class = '214' THEN 10.0
                        WHEN mf.property_class LIKE '3%' OR mf.property_class LIKE '9%' THEN GREATEST(1.0, FLOOR(mf.char_bldg_sf / 1000.0))
                        ELSE 1.0
                    END
                )
            ) AS apt_units
        FROM mf_base mf
        JOIN spatial_mapping sm ON mf.pin10 = sm.pin10
        GROUP BY sm.neighborhood_name
    ),
    all_neighborhoods AS (
        SELECT DISTINCT neighborhood_name FROM spatial_mapping
    )
    SELECT
        an.neighborhood_name AS neighborhood,
        COALESCE(c.condos, 0) AS condos,
        COALESCE(s.sfhs, 0) AS sfhs,
        CAST(COALESCE(m.apt_units, 0) AS INT) AS apartments,
        (COALESCE(c.condos, 0) + COALESCE(s.sfhs, 0) + CAST(COALESCE(m.apt_units, 0) AS INT)) AS total
    FROM all_neighborhoods an
    LEFT JOIN condo_units c ON an.neighborhood_name = c.neighborhood_name
    LEFT JOIN sfh_units s ON an.neighborhood_name = s.neighborhood_name
    LEFT JOIN mf_units_calc m ON an.neighborhood_name = m.neighborhood_name
    WHERE (COALESCE(c.condos, 0) + COALESCE(s.sfhs, 0) + COALESCE(m.apt_units, 0)) > 0
    ORDER BY total DESC;
    """

    try:
        df = con.execute(query).df()

        print(df.to_string(index=False))

    except Exception as e:
        print(f"❌ Error running analysis: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    analyze_new_builds()
