import os
import requests
import duckdb
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

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

def setup_database(config):
    print("\n📦 Loading data into DuckDB...")
    con = duckdb.connect(config['database']['file_name'])
    con.execute("INSTALL spatial; LOAD spatial;")

    table_map = {
        config['files']['chicago_zoning_geojson']: 'zoning',
        config['files']['cook_parcels_geojson']: 'parcels',
        config['files']['neighborhoods_geojson']: 'neighborhoods',
        config['files']['cta_stations_geojson']: 'transit_stops',
        config['files']['cta_bus_routes_geojson']: 'bus_routes',
        config['files']['assessor_universe_csv']: 'assessor_universe',
        config['files']['assessed_values_2023_csv']: 'assessed_values',
        config['files']['res_characteristics_csv']: 'res_characteristics',
        config['files']['parcel_addresses_csv']: 'parcel_addresses',
        config['files']['parcel_sales_csv']: 'parcel_sales',
        config['files']['building_permits_csv']: 'building_permits'
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
    config = load_config()
    for key, url in config['urls'].items():
        filename = config['files'][key]
        download_file(filename, url)
    setup_database(config)
    print("\n🚀 Ready! Now run: python3 sandbox.py")
