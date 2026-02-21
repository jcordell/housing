import markdown
from jinja2 import Template

def build_website(template_data, map_html):
    print("Compiling Markdown and HTML...")
    
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

    jinja_template = Template(markdown_content)
    populated_md = jinja_template.render(**template_data)

    article_html = markdown.markdown(populated_md, extensions=['tables'])
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
