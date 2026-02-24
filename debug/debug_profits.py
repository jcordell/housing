import duckdb
import pandas as pd
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def find_top_redevelopments():
    config = load_config()
    db_file = config['database']['file_name']

    # Grab the FAR assumptions from your config to accurately calculate SqFt Built
    far_curr = config['economic_assumptions'].get('far_current', 1.2)
    far_pritzker = config['economic_assumptions'].get('far_pritzker', 1.5)

    con = duckdb.connect(db_file)

    target_neighborhoods = (
        'WEST TOWN', 'LINCOLN PARK', 'LOGAN SQUARE',
        'NORTH CENTER', 'LAKE VIEW', 'NEAR WEST SIDE',
        'LINCOLN SQUARE'
    )

    # --- QUERY 1: Current Zoning ---
    current_query = f"""
    SELECT
        neighborhood_name AS "Neighborhood",
        prop_address AS "Address",
        acquisition_cost AS "Acquire Cost",
        tot_land_value AS "Land Cost",
        tot_bldg_value AS "Bldg Cost",
        (cost_curr - acquisition_cost) AS "Redevelop Cost",
        cost_curr AS "Total Cost",
        yield_curr AS "Units",
        (area_sqft * {far_curr}) AS "SqFt Built",
        rev_curr AS "Total Revenue",
        (rev_curr - cost_curr) AS "Profit",
        ((rev_curr - cost_curr) / NULLIF(cost_curr, 0) * 100) AS "Profit %"
    FROM step5_pro_forma
    WHERE neighborhood_name IN {target_neighborhoods}
      AND feasible_existing > 0
    ORDER BY "Profit %" DESC
    LIMIT 20;
    """

    # --- QUERY 2: Pritzker's Upzoning ---
    # We filter where either existing is feasible OR it becomes feasible under Pritzker
    pritzker_query = f"""
    SELECT
        neighborhood_name AS "Neighborhood",
        prop_address AS "Address",
        acquisition_cost AS "Acquire Cost",
        tot_land_value AS "Land Cost",
        tot_bldg_value AS "Bldg Cost",
        (cost_pritzker - acquisition_cost) AS "Redevelop Cost",
        cost_pritzker AS "Total Cost",
        yield_pritzker AS "Units",
        (area_sqft * {far_pritzker}) AS "SqFt Built",
        rev_pritzker AS "Total Revenue",
        (rev_pritzker - cost_pritzker) AS "Profit",
        ((rev_pritzker - cost_pritzker) / NULLIF(cost_pritzker, 0) * 100) AS "Profit %"
    FROM step5_pro_forma
    WHERE neighborhood_name IN {target_neighborhoods}
      AND (feasible_existing > 0 OR new_pritzker > 0)
    ORDER BY "Profit %" DESC
    LIMIT 20;
    """

    def format_output(df):
        currency_cols = [
            "Acquire Cost", "Land Cost", "Bldg Cost",
            "Redevelop Cost", "Total Cost", "Total Revenue", "Profit"
        ]
        for col in currency_cols:
            df[col] = df[col].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else "$0")

        df["SqFt Built"] = df["SqFt Built"].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df["Units"] = df["Units"].fillna(0).astype(int)
        df["Profit %"] = df["Profit %"].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "0.0%")

        return df

    try:
        print("\n" + "="*140)
        print("🏢 TOP 20 HIGHEST MARGIN REDEVELOPMENTS - CURRENT ZONING")
        print("="*140)
        df_curr = con.execute(current_query).df()
        if not df_curr.empty:
            print(format_output(df_curr).to_string(index=False))
        else:
            print("No profitable redevelopments found under current zoning.")

        print("\n" + "="*140)
        print("📈 TOP 20 HIGHEST MARGIN REDEVELOPMENTS - PRITZKER UPZONING")
        print("="*140)
        df_pritzker = con.execute(pritzker_query).df()
        if not df_pritzker.empty:
            print(format_output(df_pritzker).to_string(index=False))
        else:
            print("No profitable redevelopments found under Pritzker upzoning.")

    except Exception as e:
        print(f"❌ Error running analysis: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    find_top_redevelopments()
