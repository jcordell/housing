import duckdb
import pandas as pd
import folium
from folium.features import DivIcon
import json
import webbrowser
import os
import markdown
from jinja2 import Template

DB_FILE = "sb79_housing.duckdb"

def analyze_and_map():
    if not os.path.exists('neighborhoods.geojson'):
        print("ERROR: 'neighborhoods.geojson' missing. Run 'download-sb79.py' first.")
        return

    recalculate = os.environ.get('RECALCULATE', 'true').lower() == 'true'
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    if recalculate:
        print("Running 5-Scenario Spatial Analysis with UCLA Feasibility Filters (Caching results)...")
        con.execute("""
            CREATE OR REPLACE TEMPORARY TABLE parcel_base AS
            WITH
            target_zones AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435, zone_class FROM zoning WHERE zone_class SIMILAR TO '(RS|RT|RM|B|C).*'),
            projected_transit AS (SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM transit_stops),
            projected_bus_all AS (SELECT CAST(route AS VARCHAR) as route, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM bus_routes),
            projected_bus_hf AS (SELECT geom_3435 FROM projected_bus_all WHERE route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95')),
            projected_bus_brt AS (SELECT geom_3435 FROM projected_bus_all WHERE route = 'J14'),
            processed_parcels AS (SELECT pin10, SUBSTR(pin10, 1, 7) as block_id, ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435 FROM parcels WHERE geom IS NOT NULL),
            parcel_zone_join AS (SELECT p.pin10, p.block_id, p.geom_3435, ST_Area(p.geom_3435) as area_sqft, z.zone_class FROM processed_parcels p JOIN target_zones z ON ST_Intersects(p.geom_3435, z.geom_3435)),
            eligible_parcels AS (SELECT pin10, ANY_VALUE(block_id) as block_id, ANY_VALUE(geom_3435) as geom_3435, ANY_VALUE(area_sqft) as area_sqft, ANY_VALUE(zone_class) as zone_class FROM parcel_zone_join GROUP BY pin10),
            assembled_lots AS (
                SELECT block_id, zone_class, ST_Union_Agg(geom_3435) as assembled_geom, ST_Transform(ST_Centroid(ST_Union_Agg(geom_3435)), 'EPSG:3435', 'EPSG:4326', true) as center_geom, SUM(area_sqft) as assembled_area_sqft, COUNT(pin10) as parcels_combined
                FROM eligible_parcels GROUP BY block_id, zone_class
            ),
            parcel_bus_counts AS (
                SELECT a.block_id, a.zone_class, COUNT(DISTINCT b_all.route) as all_bus_count, COUNT(DISTINCT CASE WHEN b_all.route IN ('4', '9', '12', '14', 'J14', '20', '34', '47', '49', '53', '54', '55', '60', '63', '66', '72', '77', '79', '81', '82', '95') THEN b_all.route END) as hf_bus_count
                FROM assembled_lots a JOIN projected_bus_all b_all ON ST_Distance(a.assembled_geom, b_all.geom_3435) <= 1320 GROUP BY a.block_id, a.zone_class
            ),
            parcel_distances AS (
                SELECT a.block_id, a.center_geom, a.assembled_area_sqft as area_sqft, a.parcels_combined, a.zone_class, COALESCE(pbc.all_bus_count, 0) as all_bus_count, COALESCE(pbc.hf_bus_count, 0) as hf_bus_count, MIN(ST_Distance(a.assembled_geom, t.geom_3435)) as min_dist_train, MIN(ST_Distance(a.assembled_geom, b_brt.geom_3435)) as min_dist_brt, MIN(ST_Distance(a.assembled_geom, b_hf.geom_3435)) as min_dist_hf_bus
                FROM assembled_lots a LEFT JOIN projected_transit t ON ST_Distance(a.assembled_geom, t.geom_3435) <= 2640 LEFT JOIN projected_bus_brt b_brt ON ST_Distance(a.assembled_geom, b_brt.geom_3435) <= 2640 LEFT JOIN projected_bus_hf b_hf ON ST_Distance(a.assembled_geom, b_hf.geom_3435) <= 1320 LEFT JOIN parcel_bus_counts pbc ON a.block_id = pbc.block_id AND a.zone_class = pbc.zone_class
                GROUP BY a.block_id, a.center_geom, a.assembled_area_sqft, a.parcels_combined, a.zone_class, pbc.all_bus_count, pbc.hf_bus_count
            ),
            parcel_calculations AS (
                SELECT center_geom, area_sqft, zone_class, parcels_combined,
                    GREATEST(parcels_combined, CASE WHEN zone_class LIKE 'RS-1%' OR zone_class LIKE 'RS-2%' THEN FLOOR(area_sqft / 5000) WHEN zone_class LIKE 'RS-3%' THEN FLOOR(area_sqft / 2500) WHEN zone_class LIKE 'RT-3.5%' THEN FLOOR(area_sqft / 1250) WHEN zone_class LIKE 'RT-4%' THEN FLOOR(area_sqft / 1000) WHEN zone_class LIKE 'RM-4.5%' OR zone_class LIKE 'RM-5%' THEN FLOOR(area_sqft / 400) WHEN zone_class LIKE 'RM-6%' OR zone_class LIKE 'RM-6.5%' THEN FLOOR(area_sqft / 200) WHEN zone_class LIKE '%-1' THEN FLOOR(area_sqft / 1000) WHEN zone_class LIKE '%-2' OR zone_class LIKE '%-3' THEN FLOOR(area_sqft / 400) WHEN zone_class LIKE '%-5' OR zone_class LIKE '%-6' THEN FLOOR(area_sqft / 200) ELSE FLOOR(area_sqft / 1000) END) as current_capacity,
                    CASE WHEN zone_class IN ('RS-1', 'RS-2', 'RS-3') THEN CASE WHEN (area_sqft / parcels_combined) < 2500 THEN 1 * parcels_combined WHEN (area_sqft / parcels_combined) < 5000 THEN 4 * parcels_combined WHEN (area_sqft / parcels_combined) < 7500 THEN 6 * parcels_combined ELSE 8 * parcels_combined END ELSE 0 END as pritzker_capacity,
                    CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) WHEN min_dist_train <= 2640 OR min_dist_brt <= 1320 OR hf_bus_count >= 2 THEN FLOOR((area_sqft / 43560.0) * 100) WHEN min_dist_brt <= 2640 THEN FLOOR((area_sqft / 43560.0) * 80) ELSE 0 END as cap_true_sb79,
                    CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) WHEN min_dist_train <= 2640 THEN FLOOR((area_sqft / 43560.0) * 100) ELSE 0 END as cap_train_only,
                    CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 2640 AND min_dist_hf_bus <= 1320 THEN CASE WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_hf_bus,
                    CASE WHEN area_sqft < 5000 THEN 0 WHEN min_dist_train <= 2640 AND (min_dist_hf_bus <= 1320 OR all_bus_count >= 2) THEN CASE WHEN min_dist_train <= 1320 THEN FLOOR((area_sqft / 43560.0) * 120) ELSE FLOOR((area_sqft / 43560.0) * 100) END ELSE 0 END as cap_train_and_bus_combo
                FROM parcel_distances
            )
            SELECT center_geom, area_sqft, parcels_combined, zone_class,

                -- UCLA REDEVELOPMENT LIKELIHOOD FILTER:
                -- Net units are only counted if the new zoning yields a realistic feasibility multiplier.
                -- Missing Middle needs at least a 2x yield. Mid-rises need at least a 3x yield over existing baseline.
                CASE WHEN pritzker_capacity >= (current_capacity * 2) THEN GREATEST(0, pritzker_capacity - current_capacity) ELSE 0 END as new_pritzker,
                CASE WHEN cap_true_sb79 >= (current_capacity * 3) THEN GREATEST(0, cap_true_sb79 - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_true_sb79,
                CASE WHEN cap_true_sb79 >= (current_capacity * 3) THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_true_sb79) - current_capacity) ELSE 0 END as tot_true_sb79,

                CASE WHEN cap_train_only >= (current_capacity * 3) THEN GREATEST(0, cap_train_only - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_only,
                CASE WHEN cap_train_only >= (current_capacity * 3) THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_only) - current_capacity) ELSE 0 END as tot_train_only,

                CASE WHEN cap_train_and_hf_bus >= (current_capacity * 3) THEN GREATEST(0, cap_train_and_hf_bus - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_hf_bus,
                CASE WHEN cap_train_and_hf_bus >= (current_capacity * 3) THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_hf_bus) - current_capacity) ELSE 0 END as tot_train_and_hf_bus,

                CASE WHEN cap_train_and_bus_combo >= (current_capacity * 3) THEN GREATEST(0, cap_train_and_bus_combo - GREATEST(current_capacity, pritzker_capacity)) ELSE 0 END as add_train_and_bus_combo,
                CASE WHEN cap_train_and_bus_combo >= (current_capacity * 3) THEN GREATEST(0, GREATEST(current_capacity, pritzker_capacity, cap_train_and_bus_combo) - current_capacity) ELSE 0 END as tot_train_and_bus_combo

            FROM parcel_calculations;
        """)

        print("Extracting Neighborhood Aggregates and writing to permanent cache...")
        con.execute("""
            CREATE OR REPLACE TABLE neighborhood_results AS
            SELECT
                n.community as neighborhood_name,
                SUM(pb.new_pritzker) as new_pritzker, SUM(pb.add_true_sb79) as add_true_sb79, SUM(pb.tot_true_sb79) as tot_true_sb79,
                SUM(pb.add_train_only) as add_train_only, SUM(pb.tot_train_only) as tot_train_only,
                SUM(pb.add_train_and_hf_bus) as add_train_and_hf_bus, SUM(pb.tot_train_and_hf_bus) as tot_train_and_hf_bus,
                SUM(pb.add_train_and_bus_combo) as add_train_and_bus_combo, SUM(pb.tot_train_and_bus_combo) as tot_train_and_bus_combo,

                SUM(pb.parcels_combined) as total_parcels,
                SUM(pb.area_sqft) as total_area_sqft,
                SUM(CASE WHEN pb.zone_class NOT LIKE 'RS-1%' AND pb.zone_class NOT LIKE 'RS-2%' AND pb.zone_class NOT LIKE 'RS-3%' THEN pb.parcels_combined ELSE 0 END) as parcels_mf_zoned,
                SUM(CASE WHEN pb.zone_class NOT LIKE 'RS-1%' AND pb.zone_class NOT LIKE 'RS-2%' AND pb.zone_class NOT LIKE 'RS-3%' THEN pb.area_sqft ELSE 0 END) as area_mf_zoned,

                ST_Y(ST_Centroid(n.geom)) as label_lat, ST_X(ST_Centroid(n.geom)) as label_lon
            FROM parcel_base pb JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pb.center_geom, n.geom)
            GROUP BY n.community, n.geom HAVING SUM(pb.tot_true_sb79) > 0 OR SUM(pb.tot_train_and_bus_combo) > 0
        """)
        df_neighborhoods = con.execute("SELECT * FROM neighborhood_results ORDER BY tot_train_and_bus_combo DESC").df()
    else:
        print("RECALCULATE is false. Loading cached dataset...")
        try:
            df_neighborhoods = con.execute("SELECT * FROM neighborhood_results ORDER BY tot_train_and_bus_combo DESC").df()
        except Exception:
            print("❌ ERROR: Cached table not found or schema outdated. Please run: RECALCULATE=true python3 generate-indexhtml.py")
            con.close()
            return

    con.close()

    if df_neighborhoods.empty:
        print("No data found.")
        return

    # ---------------------------------------------------------
    # DYNAMIC RENT INCREASE ANALYSIS (ZILLOW ZORI)
    # ---------------------------------------------------------
    high_cost_nbhds = []
    try:
        if os.path.exists('zillow_rent.csv'):
            df_rent = pd.read_csv('zillow_rent.csv')
            df_chi_rent = df_rent[df_rent['City'] == 'Chicago'].copy()
            df_chi_rent['neighborhood_name'] = df_chi_rent['RegionName'].str.upper().str.replace('LAKEVIEW', 'LAKE VIEW')

            date_cols = [c for c in df_chi_rent.columns if c.startswith('20')]
            if len(date_cols) >= 61:
                latest_col = date_cols[-1]
                five_yr_col = date_cols[-61]
                df_chi_rent['rent_increase_pct'] = ((df_chi_rent[latest_col] - df_chi_rent[five_yr_col]) / df_chi_rent[five_yr_col]) * 100

                valid_nbhds = df_neighborhoods['neighborhood_name'].unique()
                df_chi_rent = df_chi_rent[df_chi_rent['neighborhood_name'].isin(valid_nbhds)]
                high_cost_nbhds = df_chi_rent.nlargest(15, 'rent_increase_pct')['neighborhood_name'].tolist()
    except Exception as e:
        print("Could not process Zillow rent data:", e)

    if not high_cost_nbhds:
        high_cost_nbhds = ['LINCOLN PARK', 'LAKE VIEW', 'NEAR NORTH SIDE', 'NEAR WEST SIDE', 'NORTH CENTER', 'WEST TOWN', 'LOGAN SQUARE', 'EDGEWATER', 'LINCOLN SQUARE']

    # Slice dataframes
    df_top15 = df_neighborhoods[df_neighborhoods['neighborhood_name'].isin(high_cost_nbhds[:15])]
    df_top5 = df_neighborhoods[df_neighborhoods['neighborhood_name'].isin(high_cost_nbhds[:5])]
    df_rest = df_neighborhoods[~df_neighborhoods['neighborhood_name'].isin(high_cost_nbhds[:15])]

    exp_pritzker = df_top15['new_pritzker'].sum()
    exp_sb79_full = df_top15['tot_true_sb79'].sum()
    exp_sb79_diff = df_top15['add_true_sb79'].sum()

    top5_pritzker = df_top5['new_pritzker'].sum()
    top5_sb79_full = df_top5['tot_true_sb79'].sum()

    pct_pritzker = (exp_pritzker / df_neighborhoods['new_pritzker'].sum()) * 100 if df_neighborhoods['new_pritzker'].sum() > 0 else 0
    pct_sb79 = (exp_sb79_full / df_neighborhoods['tot_true_sb79'].sum()) * 100 if df_neighborhoods['tot_true_sb79'].sum() > 0 else 0

    top5_pct_sqft = (df_top5['area_mf_zoned'].sum() / df_top5['total_area_sqft'].sum()) * 100 if df_top5['total_area_sqft'].sum() > 0 else 0
    rest_pct_sqft = (df_rest['area_mf_zoned'].sum() / df_rest['total_area_sqft'].sum()) * 100 if df_rest['total_area_sqft'].sum() > 0 else 0
    pct_top15_area = (df_top15['total_area_sqft'].sum() / df_neighborhoods['total_area_sqft'].sum()) * 100 if df_neighborhoods['total_area_sqft'].sum() > 0 else 0

    # ---------------------------------------------------------
    # FISCAL MODELING: PROPERTY TAX YIELD PER ACRE
    # ---------------------------------------------------------
    avg_sfh_value = 1200000
    avg_condo_value = 450000
    effective_tax_rate = 0.018

    sfh_tax_per_unit = avg_sfh_value * effective_tax_rate
    unit_tax_per_condo = avg_condo_value * effective_tax_rate

    sfh_yield_per_acre = 14 * sfh_tax_per_unit
    four_flat_yield_per_acre = (14 * 4) * unit_tax_per_condo
    midrise_yield_per_acre = 100 * unit_tax_per_condo

    tax_multiplier = midrise_yield_per_acre / sfh_yield_per_acre

    # ---------------------------------------------------------
    # TERMINAL OUTPUT
    # ---------------------------------------------------------
    print("\n" + "="*80)
    print("HOUSING POLICY IMPACT ANALYSIS: (Filtered for Redevelopment Feasibility)")
    print("="*80)
    print(f"1. Original Pritzker Upzoning (Net New):         {df_neighborhoods['new_pritzker'].sum():,.0f}")
    print("-" * 80)
    print(f"2. TRUE CA SB 79 (Trains + BRT/Bus Intersections): {df_neighborhoods['tot_true_sb79'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_true_sb79'].sum():,.0f}")
    print(f"3. SB 79: Trains Only:                           {df_neighborhoods['tot_train_only'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_only'].sum():,.0f}")
    print(f"4. SB 79 TRAIN + (HIGH FREQ BUS OR 2+ BUS LINES): {df_neighborhoods['tot_train_and_bus_combo'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}")
    print(f"5. SB 79 TRAIN + HIGH FREQ BUS:                  {df_neighborhoods['tot_train_and_hf_bus'].sum():,.0f}")
    print(f"   ↳ Additional over Pritzker:                   {df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}")
    print("="*80)
    print("EQUITY IMPACT: TOP 15 GENTRIFYING/HIGH-RENT-GROWTH NEIGHBORHOODS")
    print("-" * 80)
    print(f"Top 15 Share of Citywide Developable Land:       {pct_top15_area:.1f}%")
    print(f"Pritzker Units in these areas:                   {exp_pritzker:,.0f} ({pct_pritzker:.1f}% of total citywide)")
    print(f"SB 79 Units in these areas:                      {exp_sb79_full:,.0f} ({pct_sb79:.1f}% of total citywide)")
    print(f"↳ Extra units unlocked in exclusionary areas:    +{exp_sb79_diff:,.0f}")

    print("="*80)
    print("FISCAL IMPACT: PROPERTY TAX YIELD PER ACRE (TOP 5 NEIGHBORHOODS)")
    print("="*80)
    print(f"Single-Family Home Yield (14 units/acre):        ${sfh_yield_per_acre:,.0f} / acre")
    print(f"Pritzker 4-Flat Yield (56 units/acre):           ${four_flat_yield_per_acre:,.0f} / acre")
    print(f"SB 79 Mid-Rise Yield (100 units/acre):           ${midrise_yield_per_acre:,.0f} / acre")
    print(f"↳ Revenue Multiplier: Mid-rises generate {tax_multiplier:.1f}x more tax revenue per acre than SFH.")
    print("="*80 + "\n")

    # ---------------------------------------------------------
    # JINJA TEMPLATE DATA PREP
    # ---------------------------------------------------------
    template_data = {
        'pritzker_total': f"{df_neighborhoods['new_pritzker'].sum():,.0f}",
        'pct_pritzker': f"{pct_pritzker:.1f}",
        'true_sb79_total': f"{df_neighborhoods['tot_true_sb79'].sum():,.0f}",
        'true_sb79_diff': f"+{df_neighborhoods['add_true_sb79'].sum():,.0f}",
        'pct_sb79': f"{pct_sb79:.1f}",
        'train_only_total': f"{df_neighborhoods['tot_train_only'].sum():,.0f}",
        'train_only_diff': f"+{df_neighborhoods['add_train_only'].sum():,.0f}",
        'train_combo_total': f"{df_neighborhoods['tot_train_and_bus_combo'].sum():,.0f}",
        'train_combo_diff': f"+{df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}",
        'train_hf_total': f"{df_neighborhoods['tot_train_and_hf_bus'].sum():,.0f}",
        'train_hf_diff': f"+{df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}",
        'exp_sb79_diff': f"{exp_sb79_diff:,.0f}",
        'affordable_units': f"{exp_sb79_diff * 0.20:,.0f}",
        'top5_pct_sqft': f"{top5_pct_sqft:.1f}",
        'rest_pct_sqft': f"{rest_pct_sqft:.1f}",
        'pct_top15_area': f"{pct_top15_area:.1f}",
        'top5_pritzker': f"{top5_pritzker:,.0f}",
        'top5_sb79_full': f"{top5_sb79_full:,.0f}",
        'sfh_yield': f"${sfh_yield_per_acre:,.0f}",
        'four_flat_yield': f"${four_flat_yield_per_acre:,.0f}",
        'midrise_yield': f"${midrise_yield_per_acre:,.0f}",
        'tax_multiplier': f"{tax_multiplier:.1f}"
    }

    # ---------------------------------------------------------
    # AUTO-GENERATE MARKDOWN FILE
    # ---------------------------------------------------------
    markdown_content = """# Proposal for a TOD Amendment to the BUILD Act

Illinois is facing a severe housing shortage. Governor Pritzker’s proposed BUILD Act is a critical first step, unlocking "missing middle" housing by allowing multi-unit developments on historically restricted single-family lots. Our analysis shows the base BUILD Act could unlock **{{ pritzker_total }}** new housing units across Chicago.

This proposal analyzes adding a Transit-Oriented Development (TOD) amendment similar to California's recently passed SB79.

## The "Missing Middle" in High-Cost Areas
We analyzed the Zillow Observed Rent Index (ZORI) to identify Chicago's top 15 neighborhoods experiencing the most extreme 5-year rent spikes. Under the base BUILD Act, only **{{ pct_pritzker }}%** of new citywide housing capacity falls within these critical high-cost areas, despite them comprising **{{ pct_top15_area }}%** of the city's residential land.

Why? Because the most desirable, walkable neighborhoods in Chicago are desirable *because* they are already dense.

* In the **Top 5** highest-rent-growth neighborhoods, **{{ top5_pct_sqft }}%** of the land is *already* zoned for multi-family housing.

* Across the **rest of Chicago**, that number drops to just **{{ rest_pct_sqft }}%**.

Because land acquisition costs in these highly restricted neighborhoods are high, developers cannot afford to tear down a $1.5M single-family home just to build a 3-flat. To unlock housing in high-opportunity, transit-rich areas, we must allow mid-rise density.

In the Top 5 most expensive neighborhoods, the base BUILD Act only upzones a potential **{{ top5_pritzker }}** new units. Layering the CA SB79 transit density standard generates **{{ top5_sb79_full }}** units in those same neighborhoods.

## The Solution: A California-Style SB 79 Amendment
By adopting a transit-oriented density model similar to [California's SB 79](https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB79), we can shift where housing gets built.

SB 79 effectively legalizes **5 to 10-story mid-rise apartment buildings**, similar to the many courtyard buildings already built everywhere in Chicago, by guaranteeing baseline densities of 100 to 120 units per acre near high-frequency transit hubs. It overrides local exclusionary zoning and limits restrictive parking minimums, allowing dense, walkable communities in areas where the land values are highest.

If Illinois adopts a True CA SB 79 model allowing building near trains and bus intersections:

* Upzoning allows building **{{ true_sb79_total }}** units.

* We unlock **{{ true_sb79_diff }}** *additional* homes compared to the base BUILD Act.

* The share of new housing built in Chicago's 15 most expensive, highest-rent-growth neighborhoods nearly doubles to **{{ pct_sb79 }}%**.

## Economic & Fiscal Impact: The Property Tax Yield

Upzoning is also a fiscal boon. When we measure property tax yield per acre in Chicago's top 5 highest-rent neighborhoods, there is an obvious financial incentive for Transit-Oriented Development:

* **Single-Family Home Zone:** **{{ sfh_yield }}** in property tax revenue per acre.
* **Pritzker's 4-Flat (Missing Middle):** **{{ four_flat_yield }}** per acre.
* **SB 79 5-Story Mid-Rise:** **{{ midrise_yield }}** per acre.

By allowing mid-rise buildings near transit, the city captures nearly **{{ tax_multiplier }}x the tax revenue per acre** compared to single-family homes, expanding the tax base without raising property tax rates on existing working-class homeowners.

## New Affordable Housing Units
Chicago's Affordable Requirements Ordinance (ARO) requires upzoned properties to designate 20% of units as affordable. This upzoning permits **{{ exp_sb79_diff }}** new units strictly in the 15 most expensive neighborhoods. This amendment would mandate the private construction of **{{ affordable_units }} permanently affordable homes** in areas with the city's best schools, transit, and job access while costing taxpayers zero dollars.

## Transit Proximity Policy Options
We analyzed four different legislative requirements for triggering transit-based upzoning. We compared the base SB79 text (upzoning units near Trains OR Bus Intersections) to alternatives requiring varying levels of access to transportation.

*(Note: Data filtered for feasibility. Parcels are only counted if the upzoning allows at least a 3x yield over existing capacity).*

We calculated the following housing capacity increases for each proposal:

| Proposal Name | Nearby Transit Requirement | Total New Housing Units | Additional vs Pritzker |
| :--- | :--- | :--- | :--- |
| **1. Original Pritzker** | Baseline "missing middle" upzoning applied evenly. | **{{ pritzker_total }}** | *Baseline* |
| **2. True CA SB 79** | Train OR intersection of 2+ high-frequency buses. | **{{ true_sb79_total }}** | **{{ true_sb79_diff }}** |
| **3. Train Only** | Strictly CTA/Metra rail stations. | **{{ train_only_total }}** | **{{ train_only_diff }}** |
| **4. Train + Bus Options** | Train AND (HF bus OR any 2 bus lines). | **{{ train_combo_total }}** | **{{ train_combo_diff }}** |
| **5. Train + HF Bus** | Train AND a 10-min frequency bus stop. | **{{ train_hf_total }}** | **{{ train_hf_diff }}** |

<br>

*Use the layer toggle on the interactive map below to switch between the different transit-oriented density scenarios and see exactly how housing capacity shifts across Chicago's neighborhoods.*

*Map looks best on desktop.*
"""
    with open('article.md', 'w') as f:
        f.write(markdown_content)

    # ---------------------------------------------------------
    # MAPPING
    # ---------------------------------------------------------
    print("Generating Interactive Map...")
    with open('neighborhoods.geojson', 'r') as f:
        geo_data = json.load(f)

    df_neighborhoods['neighborhood_name'] = df_neighborhoods['neighborhood_name'].str.upper()
    unit_lookup = df_neighborhoods.set_index('neighborhood_name').to_dict('index')

    for feature in geo_data['features']:
        name = feature['properties']['community'].upper()
        stats = unit_lookup.get(name, {})
        feature['properties']['m1_val'] = f"{stats.get('new_pritzker', 0):,.0f}"
        feature['properties']['m2_val'] = f"{stats.get('tot_true_sb79', 0):,.0f}"
        feature['properties']['m2_diff'] = f"+{stats.get('add_true_sb79', 0):,.0f}"
        feature['properties']['m3_val'] = f"{stats.get('tot_train_only', 0):,.0f}"
        feature['properties']['m3_diff'] = f"+{stats.get('add_train_only', 0):,.0f}"
        feature['properties']['m4_val'] = f"{stats.get('tot_train_and_bus_combo', 0):,.0f}"
        feature['properties']['m4_diff'] = f"+{stats.get('add_train_and_bus_combo', 0):,.0f}"
        feature['properties']['m5_val'] = f"{stats.get('tot_train_and_hf_bus', 0):,.0f}"
        feature['properties']['m5_diff'] = f"+{stats.get('add_train_and_hf_bus', 0):,.0f}"

    m = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles=None)
    folium.TileLayer('CartoDB dark_matter', name='Base Map', control=False).add_to(m)

    def add_layer(title, data_col, tooltip_fields, tooltip_aliases, show_by_default=False):
        choro = folium.Choropleth(
            geo_data=geo_data, data=df_neighborhoods,
            columns=['neighborhood_name', data_col], key_on='feature.properties.community',
            fill_color='Greens', fill_opacity=0.7, line_opacity=0.2, line_color='white',
            name=title, show=show_by_default
        )
        for key in list(choro._children.keys()):
            if key.startswith('color_map'): del choro._children[key]

        for i, row in df_neighborhoods.iterrows():
            units = row[data_col]
            if units >= 1000: label_text = f"{int(round(units/1000))}k"
            elif units > 0: label_text = "<1k"
            else: continue
            label_html = f'''<div style="font-family: sans-serif; font-size: 8pt; color: white; text-shadow: 1px 1px 2px black; text-align: center; white-space: nowrap; transform: translate(-50%, -50%); pointer-events: none;">{label_text}</div>'''
            folium.map.Marker([row['label_lat'], row['label_lon']], icon=DivIcon(icon_size=(50,20), icon_anchor=(0,0), html=label_html)).add_to(choro)

        folium.GeoJsonTooltip(fields=tooltip_fields, aliases=tooltip_aliases, style="background-color: black; color: white;").add_to(choro.geojson)
        choro.add_to(m)

    # Note: Layers matched exactly to the Markdown table's new escalation order
    add_layer("1. Pritzker Upzoning", 'new_pritzker', ['community', 'm1_val'], ['Neighborhood:', 'Pritzker Units:'], False)
    add_layer("2. TRUE CA SB 79 (Train+BRT)", 'tot_true_sb79', ['community', 'm2_val', 'm2_diff'], ['Neighborhood:', 'Total SB 79 Units:', 'Difference vs Pritzker:'], True)
    add_layer("3. SB 79 Train Only", 'tot_train_only', ['community', 'm3_val', 'm3_diff'], ['Neighborhood:', 'Total Units:', 'Difference vs Pritzker:'], False)
    add_layer("4. SB 79 Train + Bus Options", 'tot_train_and_bus_combo', ['community', 'm4_val', 'm4_diff'], ['Neighborhood:', 'Total Units:', 'Difference vs Pritzker:'], False)
    add_layer("5. SB 79 Train + HF Bus", 'tot_train_and_hf_bus', ['community', 'm5_val', 'm5_diff'], ['Neighborhood:', 'Total Units:', 'Difference vs Pritzker:'], False)

    folium.LayerControl(collapsed=False).add_to(m)

    js_and_legend_injection = f"""
    <div id="custom-legend" style="
        position: absolute; bottom: 30px; right: 20px; width: 320px;
        background-color: rgba(30, 30, 30, 0.95); color: #ffffff; z-index: 9999;
        border: 1px solid #777; padding: 15px; border-radius: 8px; font-family: sans-serif;
        pointer-events: auto; box-shadow: 2px 2px 8px rgba(0,0,0,0.5);
    ">
        <h4 id="legend-title" style="margin-top: 0; margin-bottom: 10px; font-size: 16px; border-bottom: 1px solid #555; padding-bottom: 5px;">
            2. TRUE CA SB 79 (Train+BRT)
        </h4>
        <p style="margin: 0; font-size: 14px; line-height: 1.6;">
            <b>Total Net New Units:</b> <span id="legend-tot">{df_neighborhoods['tot_true_sb79'].sum():,.0f}</span><br>
            <span style="color: #4CAF50;"><b>Additional vs Pritzker:</b> <span id="legend-add">+{df_neighborhoods['add_true_sb79'].sum():,.0f}</span></span>
        </p>
    </div>
    <script>
    var layerData = {{
        "1. Pritzker Upzoning": {{ tot: "{df_neighborhoods['new_pritzker'].sum():,.0f}", add: "N/A (Baseline)" }},
        "2. TRUE CA SB 79 (Train+BRT)": {{ tot: "{df_neighborhoods['tot_true_sb79'].sum():,.0f}", add: "+{df_neighborhoods['add_true_sb79'].sum():,.0f}" }},
        "3. SB 79 Train Only": {{ tot: "{df_neighborhoods['tot_train_only'].sum():,.0f}", add: "+{df_neighborhoods['add_train_only'].sum():,.0f}" }},
        "4. SB 79 Train + Bus Options": {{ tot: "{df_neighborhoods['tot_train_and_bus_combo'].sum():,.0f}", add: "+{df_neighborhoods['add_train_and_bus_combo'].sum():,.0f}" }},
        "5. SB 79 Train + HF Bus": {{ tot: "{df_neighborhoods['tot_train_and_hf_bus'].sum():,.0f}", add: "+{df_neighborhoods['add_train_and_hf_bus'].sum():,.0f}" }}
    }};

    document.addEventListener("DOMContentLoaded", function() {{
        setTimeout(function() {{
            var checkboxes = document.querySelectorAll('.leaflet-control-layers-overlays input[type="checkbox"]');
            var spans = document.querySelectorAll('.leaflet-control-layers-overlays span');

            checkboxes.forEach(function(cb, index) {{
                cb.addEventListener('change', function() {{
                    if(this.checked) {{
                        checkboxes.forEach(function(other) {{
                            if(other !== cb && other.checked) {{
                                other.click();
                            }}
                        }});

                        var layerName = spans[index].innerText.trim();
                        if (layerData[layerName]) {{
                            document.getElementById("legend-title").innerText = layerName;
                            document.getElementById("legend-tot").innerText = layerData[layerName].tot;
                            document.getElementById("legend-add").innerText = layerData[layerName].add;
                        }}
                    }}
                }});
            }});
        }}, 500);
    }});
    </script>
    """
    m.get_root().html.add_child(folium.Element(js_and_legend_injection))
    m.get_root().render()
    map_html = m.get_root()._repr_html_()

    # ---------------------------------------------------------
    # COMPILE HTML
    # ---------------------------------------------------------
    print("Compiling Markdown and HTML...")
    with open('article.md', 'r') as f:
        md_text = f.read()

    jinja_template = Template(md_text)
    populated_md = jinja_template.render(**template_data)

    article_html = markdown.markdown(populated_md, extensions=['tables'])

    # Make the table fully responsive
    article_html = article_html.replace('<table>', '<div class="overflow-x-auto w-full"><table>').replace('</table>', '</table></div>')

    final_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Housing Policy Impact Analysis</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            .prose h1 {{ font-size: 2.25rem; font-weight: bold; margin-bottom: 1rem; color: #1f2937; line-height: 1.2; }}
            .prose h2 {{ font-size: 1.5rem; font-weight: bold; margin-top: 2rem; margin-bottom: 0.75rem; color: #374151; border-bottom: 1px solid #e5e7eb; padding-bottom: 0.5rem;}}
            .prose p {{ margin-bottom: 1rem; color: #4b5563; line-height: 1.7; }}
            .prose ul {{ list-style-type: disc; padding-left: 1.5rem; margin-bottom: 1rem; color: #4b5563; line-height: 1.7; }}
            .prose li {{ margin-bottom: 0.5rem; }}
            .prose strong {{ color: #111827; }}
            .prose table {{ width: 100%; border-collapse: collapse; margin-top: 1rem; margin-bottom: 1rem; text-align: left; min-width: 700px; }}
            .prose th {{ background-color: #f3f4f6; padding: 0.75rem; font-weight: 600; color: #374151; border: 1px solid #e5e7eb; }}
            .prose td {{ padding: 0.75rem; border: 1px solid #e5e7eb; color: #4b5563; }}
            .prose tr:nth-child(even) {{ background-color: #f9fafb; }}
        </style>
    </head>
    <body class="bg-gray-50 font-sans antialiased">
        <div class="max-w-7xl mx-auto px-4 py-8">
            <div class="flex flex-col gap-8">
                <div class="bg-white p-8 rounded-xl shadow-lg border border-gray-100">
                    <div class="prose max-w-none">
                        {article_html}
                    </div>
                </div>
                <div class="w-full h-[800px] rounded-xl shadow-lg overflow-hidden border border-gray-100 relative">
                    {map_html}
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    output_file = "index.html"
    with open(output_file, 'w') as f:
        f.write(final_html)

    print(f"✅ Success! Page saved to {output_file}")
    try:
        webbrowser.open('file://' + os.path.realpath(output_file))
    except:
        pass

if __name__ == "__main__":
    analyze_and_map()
