import requests
import duckdb
import os
import time

# --- Configuration ---
DB_FILE = "chicago_housing.duckdb"

# We define a Primary URL (Official) and a Backup URL (GitHub Mirror)
DATASETS = {
    "chicago_zoning.geojson": {
        "primary": "https://data.cityofchicago.org/api/geospatial/dj47-wfun?method=export&format=GeoJSON",
        "backup": None
    },
    "cook_parcels.geojson": {
        "primary": "https://datacatalog.cookcountyil.gov/api/geospatial/77tz-riq7?method=export&format=GeoJSON",
        "backup": None
    },
    "wards.geojson": {
        "primary": "https://data.cityofchicago.org/api/geospatial/p293-wvbd?method=export&format=GeoJSON",
        "backup": "https://raw.githubusercontent.com/uchicago-vis-pl/chicago-vis-pl.github.io/master/data/Wards_2015.geojson" # Fallback if needed
    },
    # This is the problematic file. We add a reliable GitHub mirror as backup.
    "neighborhoods.geojson": {
        "primary": "https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON",
        "backup": "https://raw.githubusercontent.com/RandomFractals/ChicagoCrimes/master/data/chicago-community-areas.geojson"
    }
}

def download_file(filename, urls):
    # 1. Clean up bad files (fixes your 53-byte error)
    if os.path.exists(filename):
        file_size = os.path.getsize(filename)
        if file_size > 50000:  # 50KB safety check
            print(f"✅ {filename} exists ({file_size / 1024:.1f} KB). Skipping.")
            return
        else:
            print(f"⚠️  {filename} is too small ({file_size} bytes). Deleting and re-downloading...")
            os.remove(filename)

    # 2. Try Primary URL
    headers = {'User-Agent': 'Mozilla/5.0 (DataProject; python-requests)'}

    print(f"⬇️  Downloading {filename} (Primary Source)...")
    try:
        r = requests.get(urls['primary'], headers=headers, stream=True, timeout=30)
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        # Validate size immediately
        if os.path.getsize(filename) < 50000:
            raise Exception("File downloaded but is too small (API Error).")

        print(f"✅ Successfully saved {filename}.")
        return

    except Exception as e:
        print(f"❌ Primary source failed: {e}")

    # 3. Try Backup URL (if primary failed)
    if urls['backup']:
        print(f"🔄 Attempting Backup Source for {filename}...")
        try:
            r = requests.get(urls['backup'], headers=headers, stream=True, timeout=30)
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Saved {filename} from backup mirror.")
        except Exception as e:
            print(f"❌ Backup source also failed: {e}")
    else:
        print("❌ No backup source available.")

def setup_database():
    print("\n📦 Loading data into DuckDB...")
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    # Map file names to table names
    table_map = {
        'chicago_zoning.geojson': 'zoning',
        'cook_parcels.geojson': 'parcels',
        'wards.geojson': 'wards',
        'neighborhoods.geojson': 'neighborhoods'
    }

    for filename, table_name in table_map.items():
        if not os.path.exists(filename):
            print(f"⚠️  Skipping '{table_name}' because {filename} is missing.")
            continue

        # Check size one last time
        if os.path.getsize(filename) < 5000:
            print(f"⚠️  Skipping '{table_name}' because file is corrupted.")
            continue

        try:
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM ST_Read('{filename}')")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"   ✅ Loaded table '{table_name}' ({count:,} rows)")
        except Exception as e:
            print(f"   ❌ Error loading '{table_name}': {e}")

    con.close()

if __name__ == "__main__":
    # 1. Download
    for filename, url_set in DATASETS.items():
        download_file(filename, url_set)

    # 2. Load
    setup_database()
    print("\n🚀 Ready! Now run: python3 script.py")
