import duckdb
import pandas as pd

def run_aggregation_debug():
    con = duckdb.connect('data/sb79_housing.duckdb')

    print("\n" + "="*90)
    print("1. MACRO MATH: The Missing Baseline in 'Total' Columns")
    print("="*90)

    macro_query = """
                  SELECT
                      SUM(feasible_existing) as "1. True Status Quo Total",
                      SUM(new_pritzker) as "2. Pritzker Marginal Bonus",
                      SUM(feasible_existing + new_pritzker) as "3. TRUE Pritzker Total",
                      SUM(tot_true_sb79) as "4. Clean SB79 Total",
                      SUM(add_true_sb79) as "5. SB79 Marginal Bonus"
                  FROM step5_pro_forma \
                  """
    df_macro = con.execute(macro_query).df()

    for col in df_macro.columns:
        df_macro[col] = df_macro[col].apply(lambda x: f"{x:,.0f}")

    print(df_macro.T.to_string(header=False))

    print("\n" + "="*90)
    print("2. PARCEL LEVEL PROOF: Seeing the columns side-by-side")
    print("="*90)

    parcel_query = """
                   SELECT
                       prop_address,
                       existing_units,
                       yield_curr as SQ_Yield,
                       yield_pritzker as Pritzker_Yield,
                       yield_sb79 as SB79_Yield,
                       feasible_existing as SQ_Net_New,
                       new_pritzker as Pritzker_Bonus,
                       tot_true_sb79 as Total_SB79_New_Units
                   FROM step5_pro_forma
                   WHERE feasible_existing > 0 AND new_pritzker > 0 AND add_true_sb79 > 0
                       LIMIT 5 \
                   """
    df_parcel = con.execute(parcel_query).df()
    print(df_parcel.to_string(index=False))

if __name__ == "__main__":
    run_aggregation_debug()
