import duckdb
import pandas as pd
import folium
from folium.plugins import HeatMap
import webbrowser
import os

DB_FILE = "chicago_housing.duckdb"
OUTPUT_MAP = "chicago_housing_potential.html"

def analyze_and_map():
    # check for required files
    if not os.path.exists('wards.geojson') or not os.path.exists('neighborhoods.geojson'):
        print("ERROR: Missing boundary files.")
        print("Please run the wget commands provided in the instructions to download 'wards.geojson' and 'neighborhoods.geojson'.")
        return

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    print("Running analysis with Neighborhood & Ward reports...")
    print("(This performs multiple spatial joins. Please wait ~30-60 seconds...)")

    query = """
    WITH
    -- 1. ZONING (RS Zones Only)
    target_zones AS (
        SELECT
            ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,
            zone_class
        FROM zoning
        WHERE zone_class IN ('RS-1', 'RS-2', 'RS-3')
    ),

    -- 2. PARCELS (Transform & Calculate Area)
    processed_parcels AS (
        SELECT
            pin10,
            -- Transform to IL State Plane (Feet) for area calc
            ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,

            -- Keep WGS84 Centroid for Ward/Neighborhood joins
            ST_Centroid(geom) as center_geom,
            ST_Y(ST_Centroid(geom)) as lat,
            ST_X(ST_Centroid(geom)) as lon
        FROM parcels
        WHERE geom IS NOT NULL
    ),

    -- 3. CALCULATE POTENTIAL
    parcel_calculations AS (
        SELECT
            p.pin10,
            p.lat, p.lon, p.center_geom,
            ST_Area(p.geom_3435) as area_sqft,
            z.zone_class,

            1 as current_units,
            CASE
                WHEN ST_Area(p.geom_3435) < 2500 THEN 1
                WHEN ST_Area(p.geom_3435) >= 2500 AND ST_Area(p.geom_3435) < 5000 THEN 4
                WHEN ST_Area(p.geom_3435) >= 5000 AND ST_Area(p.geom_3435) < 7500 THEN 6
                WHEN ST_Area(p.geom_3435) >= 7500 THEN 8
                ELSE 1
            END as proposed_units

        FROM processed_parcels p, target_zones z
        WHERE ST_Intersects(p.geom_3435, z.geom_3435)
    ),

    -- 4. JOIN CONTEXT (Wards & Neighborhoods) AND FILTER
    final_dataset AS (
        SELECT
            pc.lat, pc.lon,
            (pc.proposed_units - pc.current_units) as net_new_units,

            -- Spatial Join: Check which Ward the center point falls into
            w.ward,

            -- Spatial Join: Check which Neighborhood the center point falls into
            n.community as neighborhood

        FROM parcel_calculations pc
        LEFT JOIN ST_Read('wards.geojson') w ON ST_Intersects(pc.center_geom, w.geom)
        LEFT JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pc.center_geom, n.geom)

        WHERE (pc.proposed_units - pc.current_units) > 0
    )
    SELECT * FROM final_dataset
    """

    try:
        df = con.execute(query).df()
    except Exception as e:
        print(f"Error executing query: {e}")
        con.close()
        return

    con.close()

    if df.empty:
        print("No data found. Check your input tables.")
        return

    # --- REPORTING ---
    total_units = df['net_new_units'].sum()
    print("\n" + "="*50)
    print(f"TOTAL POTENTIAL NEW UNITS: {total_units:,.0f}")
    print("="*50)

    print("\nTOP 20 NEIGHBORHOODS (Community Areas):")
    print("-" * 40)
    print(df.groupby('neighborhood')['net_new_units'].sum().sort_values(ascending=False).head(20))

    print("\nTOP 20 WARDS:")
    print("-" * 40)
    print(df.groupby('ward')['net_new_units'].sum().sort_values(ascending=False).head(20))

    # --- MAPPING (Aggregated) ---
    print("\nGenerating Optimized Heatmap...")

    # AGGREGATION LOGIC:
    # Round lat/lon to 3 decimal places (approx 100 meters or 1 city block).
    # This groups nearby parcels into single map points, reducing 300k+ dots to ~15k dots.
    # This prevents the browser from crashing while keeping the visual heatmap identical.

    df['lat_bin'] = df['lat'].round(3)
    df['lon_bin'] = df['lon'].round(3)

    map_data = df.groupby(['lat_bin', 'lon_bin'])['net_new_units'].sum().reset_index()
    heat_data = map_data[['lat_bin', 'lon_bin', 'net_new_units']].values.tolist()

    print(f"Compressed {len(df)} parcels into {len(map_data)} map points.")

    m = folium.Map(location=[41.8781, -87.6298], zoom_start=11, tiles="CartoDB dark_matter")

    HeatMap(
        heat_data,
        radius=14,    # Increased radius slightly since points are aggregated
        blur=18,      # Smoother blur for the "block-level" aggregate
        max_zoom=13,
        gradient={0.1: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.9: 'yellow', 1.0: 'red'}
    ).add_to(m)

    m.save(OUTPUT_MAP)
    print(f"Map saved to {OUTPUT_MAP}")

    try:
        webbrowser.open('file://' + os.path.realpath(OUTPUT_MAP))
    except:
        print("Could not auto-open browser.")

if __name__ == "__main__":
    analyze_and_map()
