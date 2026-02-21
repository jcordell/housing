import argparse
import webbrowser
import os
from calculate_parcels import run_parcel_calculations
from analyze_economics import run_analysis
from generate_map import build_map
from generate_html import build_website

def main():
    parser = argparse.ArgumentParser(description="Housing Policy Impact Analyzer Pipeline")
    parser.add_argument('--recalculate', action='store_true', help="Recalculate ALL spatial data (Slow)")
    parser.add_argument('--filter-only', action='store_true', help="Only re-apply the feasibility filters to existing spatial data (Fast)")
    parser.add_argument('--no-browser', action='store_true', help="Do not automatically open the browser at the end")
    args = parser.parse_args()

    # Data Engine Routing
    if args.recalculate:
        run_parcel_calculations(full_recalculate=True)
    elif args.filter_only:
        run_parcel_calculations(full_recalculate=False)

    # Step 2: Economics and Analysis
    df, context = run_analysis()
    if df is None or context is None:
        print("Analysis failed. Ensure you have run with --recalculate to build the database.")
        return

    # Step 3: Mapping
    map_html = build_map(df)

    # Step 4: Final HTML Compilation
    build_website(context, map_html)

    # Step 5: View Result
    if not args.no_browser:
        try:
            webbrowser.open('file://' + os.path.realpath("index.html"))
        except:
            pass

if __name__ == "__main__":
    main()
