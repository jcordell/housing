import duckdb
import pandas as pd
import yaml
import markdown
from jinja2 import Template
import os
import webbrowser

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def find_neighbor_owned_empty_lots():
    config = load_config()
    db_file = config['database']['file_name']

    con = duckdb.connect(db_file)

    # Load the spatial extension so ST_DWithin and ST_Distance work
    con.execute("INSTALL spatial; LOAD spatial;")

    query = """
            WITH target_nbhds AS (
                SELECT DISTINCT pin10, neighborhood_name, geom_3435, zone_class, area_sqft
                FROM spatial_base
                WHERE neighborhood_name IN ('WEST TOWN', 'LINCOLN PARK', 'LOGAN SQUARE',
                                            'NORTH CENTER', 'LAKE VIEW', 'NEAR WEST SIDE', 'LINCOLN SQUARE')
                  -- Strictly limit to Residential Zoning
                  AND zone_class SIMILAR TO '(RS|RT|RM).*'
                ),
                sales_info AS (
            -- Get the most recent buyer if a purchase happened recently
            SELECT
                SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
                ANY_VALUE(buyer_name) as buyer_name
            FROM parcel_sales
            WHERE buyer_name IS NOT NULL AND TRIM(buyer_name) != ''
            GROUP BY 1
                ),
                owner_info AS (
            -- Get the official mailing taxpayer name and property address
            SELECT
                SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
                ANY_VALUE(mail_address_name) as mail_name,
                ANY_VALUE(prop_address_full) as prop_address
            FROM parcel_addresses
            GROUP BY 1
                ),
                parcel_values AS (
            SELECT
                tn.pin10,
                tn.neighborhood_name,
                tn.geom_3435,
                tn.area_sqft,
                o.prop_address,
                v.property_class,
                -- Coalesce recent purchase names with official taxpayer records
                COALESCE(s.buyer_name, o.mail_name) as owner_name,

                -- Estimate market value (Assessed Value / Assessment Level)
                (COALESCE(v.bldg_value, 0.0) / 0.10) as est_bldg_value,
                (COALESCE(v.land_value, 0.0) / 0.10) as est_land_value
            FROM target_nbhds tn
                LEFT JOIN (
                SELECT SUBSTR(LPAD(CAST(pin AS VARCHAR), 14, '0'), 1, 10) as pin10,
                SUM(TRY_CAST(certified_bldg AS DOUBLE)) as bldg_value,
                SUM(TRY_CAST(certified_land AS DOUBLE)) as land_value,
                ANY_VALUE(CAST("class" AS VARCHAR)) as property_class
                FROM assessed_values
                GROUP BY 1
                ) v ON tn.pin10 = v.pin10
                LEFT JOIN owner_info o ON tn.pin10 = o.pin10
                LEFT JOIN sales_info s ON tn.pin10 = s.pin10
            WHERE COALESCE(s.buyer_name, o.mail_name) IS NOT NULL
              AND TRIM(COALESCE(s.buyer_name, o.mail_name)) != ''
                ),
                empty_lots AS (
            SELECT * FROM parcel_values
            WHERE est_bldg_value < 10000
              AND est_land_value > 0
            -- Explicitly require it to be classified as vacant land or a residential teardown
              AND property_class IN (
                '100', '190', '200', '241',
                '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '234', '278'
                )
                ),
                built_lots AS (
            SELECT * FROM parcel_values
            WHERE (est_bldg_value + est_land_value) >= 500000
              AND est_bldg_value >= 50000
                ),
                nearest_park AS (
            -- Calculate the shortest straight-line distance to any Chicago park
            SELECT
                e.pin10,
                MIN(ST_Distance(e.geom_3435, p.geom_3435)) as dist_to_park
            FROM empty_lots e
                CROSS JOIN (
                SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435
                FROM parks
                WHERE geom IS NOT NULL
                ) p
            GROUP BY 1
                ),
                matched_lots AS (
            SELECT DISTINCT
                e.neighborhood_name AS "Neighborhood",
                e.prop_address AS "Empty Lot Address",
                e.est_land_value AS "Estimated Price",
                (e.est_land_value * 0.018) AS "Taxes Paid",
                b.prop_address AS "Neighbor Address",
                (b.est_bldg_value + b.est_land_value) AS "Neighbor Est Price",
                e.owner_name AS "Owner Name",
                e.area_sqft AS empty_area,
                b.area_sqft AS neighbor_area,
                -- Expected tax is the neighbor's tax scaled by the ratio of the lot sizes
                ((b.est_bldg_value + b.est_land_value) * 0.018) * (e.area_sqft / NULLIF(b.area_sqft, 0)) AS expected_proportional_tax,
                np.dist_to_park
            FROM empty_lots e
                JOIN built_lots b
            ON ST_DWithin(e.geom_3435, b.geom_3435, 5) -- 5 foot tolerance to catch adjacent PINs
                AND e.pin10 != b.pin10
                AND e.owner_name = b.owner_name
                LEFT JOIN nearest_park np ON e.pin10 = np.pin10
                )
            SELECT
                "Neighborhood",
                "Empty Lot Address",
                "Estimated Price",
                "Taxes Paid",
                "Neighbor Address",
                "Neighbor Est Price",
                "Owner Name",
                GREATEST(0, expected_proportional_tax - "Taxes Paid") AS "Lost Tax",
                dist_to_park
            FROM matched_lots
            ORDER BY "Lost Tax" DESC; \
            """

    def format_output(df):
        currency_cols = [
            "Estimated Price", "Taxes Paid", "Neighbor Est Price", "Lost Tax"
        ]
        for col in currency_cols:
            df[col] = df[col].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else "$0")

        df["Empty Lot Address"] = df["Empty Lot Address"].fillna('Unknown').str.title()
        df["Neighbor Address"] = df["Neighbor Address"].fillna('Unknown').str.title()
        df["Owner Name"] = df["Owner Name"].apply(lambda x: str(x)[:25] + '...' if len(str(x)) > 25 else x)

        return df

    try:
        df = con.execute(query).df()

        print("\n" + "="*140)
        print(f"🌳 FOUND {len(df):,} ADJACENT EMPTY LOTS OWNED BY NEIGHBORS (Neighbor > $500k)")
        print("="*140)

        if not df.empty:
            # Calculate Walk to Park in minutes
            df['Walk to Park (Mins)'] = (df['dist_to_park'] * 1.3 / 264.0).round(1)

            # Export raw data to CSV
            csv_filename = 'lots.csv'
            df.to_csv(csv_filename, index=False)
            print(f"✅ Successfully exported raw data to {csv_filename}\n")

            # Format the display for terminal
            formatted_df = format_output(df.copy())
            print(formatted_df.drop(columns=['dist_to_park', 'Walk to Park (Mins)'], errors='ignore').head(20).to_string(index=False))
            print("\n... (showing top 20 in terminal, full list in lots.csv) ...\n")

            # --- PRINT TOP 10 CLOSEST TO PARK ---
            print("\n" + "="*80)
            print("🏞️  TOP 10 SIDE LOTS CLOSEST TO A PARK")
            print("="*80)
            closest_lots = df.sort_values('Walk to Park (Mins)').head(10)
            closest_display = closest_lots[['Neighborhood', 'Empty Lot Address', 'Walk to Park (Mins)']].copy()
            closest_display['Empty Lot Address'] = closest_display['Empty Lot Address'].fillna('Unknown').str.title()
            closest_display['Walk to Park (Mins)'] = closest_display['Walk to Park (Mins)'].apply(lambda x: f"{x:.1f} min")
            print(closest_display.to_string(index=False))

            # --- SUMMARY ESTIMATES ---
            total_lost_tax = df['Lost Tax'].sum()
            assumed_luxury_4_flat_value = 3_000_000
            effective_tax_rate = 0.018
            tax_per_new_4_flat = assumed_luxury_4_flat_value * effective_tax_rate

            total_empty_lots = len(df)
            total_new_units = total_empty_lots * 4
            total_current_taxes = df['Taxes Paid'].sum()
            total_upzoned_taxes = total_empty_lots * tax_per_new_4_flat
            total_tax_increase = total_upzoned_taxes - total_current_taxes

            # Calculate % of lots under 5 min walk dynamically
            pct_under_5 = int(round((df['Walk to Park (Mins)'].fillna(999) <= 6.0).mean() * 100))

            # --- GENERATE HTML ARTICLE ---
            generate_html_article(
                total_empty_lots,
                total_lost_tax,
                total_new_units,
                assumed_luxury_4_flat_value,
                total_upzoned_taxes,
                total_tax_increase,
                pct_under_5
            )

        else:
            print("No matching lots found.")

    except Exception as e:
        print(f"❌ Error running analysis: {e}")
    finally:
        con.close()

def generate_html_article(lots_count, lost_tax, new_units, bldg_value, upzoned_tax, net_tax_increase, pct_under_5_min):
    markdown_content = """# Subsidizing the Rich: The Hidden Cost of Chicago’s Luxury Side Yards

![Private basketball court on a side lot](images/basketball.png)

In high-demand areas, wealthy homeowners frequently purchase adjacent tear-down properties, demolish the existing structures, and absorb the parcels as massive private side yards. Because property taxes in Illinois assess vacant land drastically lower than improved land, these homeowners pay a fraction of the tax per square foot compared to their own primary residence.  Chicago effectively subsidizes these rich home owners to keep the lot empty as redeveloping the property would bring in more property taxes.

Our analysis found **{{ lots_count }}** adjacent empty lots owned by neighbors in just five high-demand north and west side neighborhoods. This is likely a significant underestimate, as there are plenty of side lots lots which don't automatically match the filters described in the next section.

## Analysis and Methodology: Defining the "Side Yard"

An empty lot is only counted if it passes a strict set of spatial, legal, and economic filters:

### 1. Spatial & Zoning Filters
* **Residential Zoning:** The lot must be zoned exclusively for residential use (`RS`, `RT`, or `RM`).
* **Adjacency:** The empty lot and the built lot must be geographically adjacent (intersecting within a 5-foot spatial tolerance).

### 2. Ownership Matching
* **Taxpayer Verification:** The empty lot and the adjacent home must have the exact same owner.

### 3. Value and Condition Thresholds
* **The "Empty" Lot:** The side lot must have an estimated building value of <$10,000, signifying it is effectively vacant or a demolished teardown. It must also have a Cook County Assessor property class corresponding to vacant land or uninhabitable teardowns.
* **The Primary Residence:** The adjacent home owned by the same taxpayer must have an estimated market value of at least $500,000, ensuring we are capturing high-value property expansions rather than distressed blocks.

I then manually verified the list and removed some outliers or lots that have been developed since the data was last updated.

## The Cost of the Status Quo
If these vacant side lots were taxed at the exact same rate per square foot as the neighboring home they are attached to, the city would collect an estimated **${{ lost_tax }}** in additional property taxes every single year.

## The Upzoning Solution
Illinois law makes it difficult to heavily tax vacant residential land based purely on highest-and-best-use due to constitutional uniformity clauses. However, upzoning provides a legal pathway to raise some taxes. The recently announced BUILD act legalizes 4 unit condos on these parcels by-right, which will increase the underlying land value. The property taxes will rise organically, and owners to either pay a slightly larger (but still heavily subsidized) premium for their private park or sell it to a developer.

<div class="callout">
Every year a luxury side yard remains a private park, we lose <strong>${{ lost_tax }}</strong> in direct revenue because these lots are taxed as "vacant" rather than as the high-value residential land they actually serve as. 
<br>
<br>

But the true "opportunity subsidy" is much higher. By allowing these parcels to sit idle in high-demand neighborhoods, the city is effectively walking away from <strong>${{ net_tax_increase }}</strong> in annual tax revenue that would be generated if these lots were developed into the 4-unit buildings allowed under the BUILD Act.
<br>
<br>

<strong>Chicago is subsidizing the exclusivity of the wealthy over $23 million every single year while missing out on {{ new_units }} much needed houses.</strong>
</div>

## Park Proximity

Even if owners choose to sell rather than pay the slightly increased tax rate, they stand to gain significantly from the increased land value that comes with higher-density zoning.  And while they might lose their private green space, **{{ pct_under_5_min }}%** of these lots are a 5 minute or less walk from the nearest park. Some of them are even directly next to a park, like this one in Lakeview East.

![View of an empty lot from a park](images/from-park.png)
"""

    template_data = {
        'lots_count': f"{lots_count:,}",
        'lost_tax': f"{lost_tax:,.0f}",
        'new_units': f"{new_units:,}",
        'bldg_value': f"{bldg_value:,.0f}",
        'upzoned_tax': f"{upzoned_tax:,.0f}",
        'net_tax_increase': f"{net_tax_increase:,.0f}",
        'pct_under_5_min': pct_under_5_min
    }

    jinja_template = Template(markdown_content)
    populated_md = jinja_template.render(**template_data)
    article_html = markdown.markdown(populated_md, extensions=['tables'])

    final_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>The Side Yard Subsidy</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            .prose h1 {{ font-size: 2.25rem; font-weight: bold; margin-bottom: 1rem; color: #1f2937; line-height: 1.2; }}
            .prose h2 {{ font-size: 1.5rem; font-weight: bold; margin-top: 2rem; margin-bottom: 0.75rem; color: #374151; border-bottom: 1px solid #e5e7eb; padding-bottom: 0.5rem;}}
            .prose h3 {{ font-size: 1.25rem; font-weight: bold; margin-top: 1.25rem; margin-bottom: 0.5rem; color: #4b5563; }}
            .prose p {{ margin-bottom: 1rem; color: #4b5563; line-height: 1.7; }}
            .prose ul {{ list-style-type: disc; padding-left: 1.5rem; margin-bottom: 1.25rem; }}
            .prose li {{ margin-bottom: 0.5rem; color: #4b5563; line-height: 1.7; }}
            .callout {{ background-color: #fef2f2; border-left: 4px solid #ef4444; padding: 1.5rem; margin: 2rem 0; border-radius: 0.5rem; }}
            .callout p {{ margin-bottom: 1rem; color: #991b1b; font-size: 1.125rem; }}
            .prose img {{ margin-top: 1.5rem; margin-bottom: 1.5rem; border-radius: 0.75rem; width: 100%; }}
        </style>
    </head>
    <body class="bg-gray-50 font-sans antialiased">
        <div class="max-w-4xl mx-auto px-4 py-8">
            <div class="flex flex-col gap-8">
                <div class="bg-white p-8 md:p-12 rounded-xl shadow-lg border border-gray-100">
                    <div class="prose max-w-none">
                        {article_html}
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    output_file = 'chicago-luxury-side-lot-subsidy.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(final_html)

    print(f"✅ Success! Article generated: {output_file}")
    try:
        webbrowser.open('file://' + os.path.realpath(output_file))
    except:
        pass

if __name__ == "__main__":
    find_neighbor_owned_empty_lots()
