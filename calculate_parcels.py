import duckdb
import yaml
import time
from jinja2 import Template

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_parcel_calculations(full_recalculate=True, is_sandbox=False):
    config = load_config()
    db_file = config['database']['file_name']

    con = duckdb.connect(db_file)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("PRAGMA enable_progress_bar;")

    if full_recalculate:
        print("\n🚀 Running Full Spatial Analysis...")

        t0 = time.time()
        print("⏳ [1/5] Isolating parcels and calculating spatial intersections...", end="", flush=True)
        with open('sql/01_spatial_joins.sql', 'r') as f:
            template = Template(f.read())
        con.execute(template.render(is_sandbox=is_sandbox, files=config['files']))
        print(f" ✅ ({time.time() - t0:.1f}s)")

        t0 = time.time()
        print("⏳ [2/5] Calculating dynamic property values and sales multipliers...", end="", flush=True)
        with open('sql/02_calculate_sales_ratios.sql', 'r') as f:
            con.execute(f.read())
        print(f" ✅ ({time.time() - t0:.1f}s)")

        t0 = time.time()
        print("⏳ [3/5] Calculating dynamic new-build condo prices...", end="", flush=True)
        with open('sql/02b_calculate_condo_values.sql', 'r') as f:
            template = Template(f.read())
        con.execute(template.render(**config['economic_assumptions']))
        print(f" ✅ ({time.time() - t0:.1f}s)")

    else:
        print("\n🚀 Skipping spatial rebuild, applying financial filters...")

    t0 = time.time()
    print("⏳ [4/5] Executing Real Estate Pro Forma...", end="", flush=True)
    with open('sql/03_pro_forma.sql', 'r') as f:
        template = Template(f.read())
    con.execute(template.render(**config['economic_assumptions']))
    print(f" ✅ ({time.time() - t0:.1f}s)")

    t0 = time.time()
    print("⏳ [5/5] Aggregating Neighborhood Results...", end="", flush=True)
    with open('sql/04_aggregate_results.sql', 'r') as f:
        con.execute(f.read())
    print(f" ✅ ({time.time() - t0:.1f}s)")

    con.close()
