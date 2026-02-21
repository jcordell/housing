import os
import requests
import duckdb

DB_FILE = "sb79_housing.duckdb"

DATASETS = {
    'chicago_zoning.geojson': 'https://data.cityofchicago.org/api/geospatial/djph-xxwh?method=export&format=GeoJSON',
    'neighborhoods.geojson': 'https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=GeoJSON',
    'cta_stations.geojson': 'https://data.cityofchicago.org/api/geospatial/8pix-ypme?method=export&format=GeoJSON',
    'cta_bus_routes.geojson': 'https://data.cityofchicago.org/api/geospatial/6uva-a5ei?method=export&format=GeoJSON',
    'zillow_rent.csv': 'https://files.zillowstatic.com/research/public_csvs/zori/Neighborhood_zori_uc_sfrcondomfr_sm_month.csv',
    'assessor_universe.csv': 'https://datacatalog.cookcountyil.gov/api/views/pabr-t5kh/rows.csv?accessType=DOWNLOAD',
    'assessed_values_2023.csv': 'https://datacatalog.cookcountyil.gov/resource/uzyt-m557.csv?$where=year=2023&$limit=2000000',

    # NEW: Corrected ID for Cook County Residential Characteristics (Age, Square Footage)
    'res_characteristics.csv': 'https://datacatalog.cookcountyil.gov/resource/x54s-btds.csv?$where=year=2023&$limit=2000000'
}

def download_file(filename, url):
    if os.path.exists(filename) and os.path.getsize(filename) > 50000:
        print(f"✅ {filename} exists. Skipping.")
        return

    print(f"⬇️  Downloading {filename}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (DataProject; python-requests)'}
        r = requests.get(url, headers=headers, stream=True, timeout=60)
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Successfully saved {filename}.")
    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")

def setup_database():
    print("\n📦 Loading data into DuckDB...")
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    table_map = {
        'chicago_zoning.geojson': 'zoning',
        'cook_parcels.geojson': 'parcels',
        'neighborhoods.geojson': 'neighborhoods',
        'cta_stations.geojson': 'transit_stops',
        'cta_bus_routes.geojson': 'bus_routes',
        'assessor_universe.csv': 'assessor_universe',
        'assessed_values_2023.csv': 'assessed_values',
        'res_characteristics.csv': 'res_characteristics'
    }

    for filename, table_name in table_map.items():
        if not os.path.exists(filename):
            print(f"⚠️  Skipping '{table_name}' because {filename} is missing.")
            continue
        try:
            if filename.endswith('.csv'):
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{filename}', ignore_errors=true)")
            else:
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM ST_Read('{filename}')")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"   ✅ Loaded table '{table_name}' ({count:,} rows)")
        except Exception as e:
            print(f"   ❌ Error loading '{table_name}': {e}")

    con.close()

if __name__ == "__main__":
    for filename, url in DATASETS.items():
        download_file(filename, url)
    setup_database()
    print("\n🚀 Ready! Now run: python3 main.py --recalculate")
