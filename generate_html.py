import markdown
import yaml
from jinja2 import Template

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def build_website(template_data, map_html):
    print("Compiling Markdown and HTML...")
    config = load_config()

    markdown_content = """# Proposal for a TOD Amendment to the BUILD Act

Illinois is facing a severe housing shortage. Governor Pritzker’s proposed BUILD Act is a critical first step, unlocking "missing middle" housing by allowing multi-unit developments on historically restricted single-family lots. Our analysis shows the base BUILD Act could unlock **{{ pritzker_total }}** new housing units across Chicago.

This proposal analyzes adding a Transit-Oriented Development (TOD) amendment similar to California's recently passed SB79.

SB 79 effectively legalizes **5 to 10-story mid-rise apartment buildings**, similar to the many courtyard buildings already built everywhere in Chicago, by guaranteeing baseline densities of 100 to 120 units per acre near high-frequency transit hubs. It overrides local exclusionary zoning and limits restrictive parking minimums, allowing dense, walkable communities in areas where the land values are highest. It also **increases Floor Area Ratio (FAR)** to between 2.5 to 4.5 depending on distance to the nearest transit stop.

## Analysis and Methodology: Defining "Feasible" Redevelopment

This proposal analyzes how many multi family homes are currently profitable to build and compares the number of currently profitable multifamily units to different zoning scenarios.
It filters to "likely to be redeveloped" properties and estimates redevelopment profitability with realistic building costs + sale revenue to determine the number of buildable units.

To ensure our model reflects real-world market conditions rather than purely theoretical maximums, a parcel is only counted as a "feasible un-built multifamily lot" if it passes a strict set of physical, legal, and economic filters:

### 1. Spatial & Zoning Filters

To be considered for evaluation, a parcel must pass baseline physical and legal constraints:

* **Allowed Zoning:** The parcel must intersect with zoning classes starting with `RS`, `RT`, `RM`, `B`, or `C` (Residential, Business, Commercial).

* **Restricted Zoning:** Parcels in Open Space (`OS`), Parks (`POS`), or Planned Manufacturing Districts (`PMD`) are explicitly excluded.

* **Maximum Lot Size:** The parcel must be 1 acre or smaller (`area_sqft <= 43560`).

* **Existing Density (FAR):** The existing Floor Area Ratio (Existing Building SqFt / Total Lot SqFt) must be `< 1.5`. If the property is already densely built, it is removed from consideration.

### 2. Property Type & Usage Exclusions

The pipeline evaluates Cook County Assessor classes (`primary_prop_class`) to prevent the demolition of critical or un-developable infrastructure:

* **Null Check:** The property class must be known (not `NULL` or `UNKNOWN`).

* **Condominium Protection:** Existing condos are completely excluded. This filters out primary condos (`299`) and secondary condo pins (`299%`).

* **Institutional & Civic Exemptions:** Exempt properties/government (`EX`), Not-for-profits (`4`), Rail/Right-of-Way/Vacant (`0`, `0%`, `1`, `1%`), and heavy industrial (`93`) are skipped.

* **Religious Buildings:** Any parcel with an address containing "CHURCH" or "RELIGIOUS" is explicitly filtered out.

### 3. Age, Condition, and Minimum Value

Parcels must meet age and value thresholds to ensure the model isn't tearing down brand-new homes or counting anomalous tax pins:

* **Minimum Value:** The combined assessed building and land value must be >$1,000 to filter out "sliver" lots or administrative anomalies.

* **Age Requirement:** The existing structure must be at least 35 years old. 

* **Empty/Low-Value Exception:** A lot can have an age of 0 *only* if its total building value is less than $250,000, signifying it is effectively vacant or contains a tear-down structure.

* **Maximum Existing Units:** The current structure must have fewer than 40 units to prevent modeling the demolition of large, currently viable apartment complexes.

### 4. Meaningful Density Increase

A developer won't rebuild to add a single unit or a few extra square feet. The project must meet minimum scale jumps:

* **Unit Multiplier:** The proposed new unit capacity must be at least double the existing unit count (`>= GREATEST(existing_units, 1.0) * 2.0`).

* **Square Footage Multiplier:** The proposed total gross square footage (GSF) must be at least 25% larger than the existing structure (`>= GREATEST(existing_sqft, 1.0) * 1.25`).

### 5. Revenue Projections (Price Per SqFt)

Revenue is calculated dynamically by looking at recent real-world sales of comparable new-build units:

* **Data Sourcing:** The model pulls multi-unit/condo sales from the last 2 years.

* **Quality Filters:** It removes bulk/multi-parcel sales, properties built before **2018**, units under 400 sqft, and anomalous sales under $50,000.

* **Neighborhood Premium:** It calculates the **80th percentile** Price-Per-SqFt (PPSF) specific to that neighborhood, requiring a minimum of 10 valid sales. Data is bounded to realistic ranges ($100 to $1,200/sqft).

* **Fallback Logic:** If a neighborhood lacks 10 recent new-build sales, it falls back to a Regional Median (North, West, Central, etc).

* **Gross Revenue:** Calculated as Total Net Rentable Area (NRA) x Local Condo PPSF.

### 6. Acquisition Cost Modeling

Because Cook County Assessor values often lag behind the actual market, the model applies dynamic corrections to estimate true land acquisition costs:

* **Base Valuation:** It takes the Assessor's certified building and land values, adjusting them by the statutory level of assessment (10% for most residential, 25% for commercial).

* **Market Correction Multiplier:** The pipeline compares recent arms-length sales (>$20k) against their assessed values to find the local under-assessment ratio (bounded between 0.5x and 3.5x). It calculates a median multiplier per neighborhood and property category. The fallback is 1.40x.

* **Dynamic Acquisition Floor:** Instead of a flat floor, the pipeline analyzes recent "teardown" sales in the area to establish a local land-value floor. 

  * If local median land is >$150/sqft, it uses the 5th percentile of local teardown prices.
  
  * If >$75/sqft, it uses the 15th percentile.
  
  * Otherwise, it uses the 30th percentile (with a final hard fallback of $20/sqft).
  
* **Final Acquisition Cost:** Takes the *greater* of (Assessor Value x Market Correction Multiplier) OR (Lot SqFt x Dynamic Acquisition Floor).

* **Disinvestment Exception:** Specific historically disinvested South/West side neighborhoods using vacant/small property classes (100, 241, 242) bypass the multiplier and are assessed at a strict 1.0x ratio to reflect localized market realities. Vacant lots from these neighborhoods are set to be acquired for $1 as possible when buying from the city.

### 7. Construction Cost Modeling & Single-Stair Efficiency

Construction costs are scaled geographically, and Net Rentable Area (NRA) is adjusted based on modern building code assumptions:

* **Base Hard Costs:** High-cost areas (Lincoln Park, Lake View, Near North Side, Loop, Near West Side) are modeled at $300/sqft. All other neighborhoods are modeled at $240/sqft.

* **Gross vs. Net Efficiency (The Double-Stair Penalty):** Because developers must build non-rentable space (hallways, lobbies, stairs), building size is reduced to calculate revenue-generating NRA. Under current Chicago zoning, dual staircases severely penalize missing-middle housing:

  * <2 units: 90% efficiency
  
  * 3 to 4 units: 75% efficiency *(High penalty for double-stair requirements)*
  
  * 5 to 9 units: 78% efficiency
  
  * 10 to 19 units: 80% efficiency
  
  * 20+ units: 82% efficiency
  
* **Pritzker & SB79 Reform (Single-Stair Bump):** Under the upzoning scenarios, the model assumes single-stairway reform is adopted, massively improving building efficiency for mid-sized structures:

  * <2 units: 90% efficiency
  
  * 3 to 6 units: **87% efficiency**
  
  * 7 to 15 units: **85% efficiency**
  
  * 16+ units: 82% efficiency
  
* **Total Cost:** Estimated Total Cost = Final Acquisition Cost + (Total GSF x Base Hard Cost).

### 8. Pro Forma Execution & Waterfall Logic

To be marked as "feasible," the numbers must pencil out for a developer, and the pipeline must pick the most logical zoning scenario to apply:

### 9. Affordable Requirements Ordinance (ARO)
Modeling has not yet included the ARO in these calculations. To prevent severely overcounting high unit buildings in this analysis, and to keep the focus on "missing middle", we set a cap of any redevelopment would have *max 20 units*.

* **Profit Hurdle:** The Estimated Total Revenue must exceed the Estimated Total Cost by at least a 15% margin (`target_profit_margin = 1.15`).

* **Highest and Best Use (HBU) Ratchet:** The model evaluates profitability across all baseline scenarios (Current Zoning, Pritzker, SB79). Parcels fall through a waterfall logic that strictly prioritizes **absolute profit maximization**:
  * A denser zoning policy only overrides a less dense baseline *if* the developer's absolute net profit increases (`profit_new > max_profit_current`).

## Transit Proximity Policy Options
We analyzed four different legislative requirements for triggering transit-based upzoning. We compared the base SB79 text (upzoning units near Trains OR Bus Intersections) to alternatives requiring varying levels of access to transportation.

*(Note: Data filtered for financial feasibility. Parcels are only counted if the zoning allows a significant multiplier over existing capacity).*

We calculated the following housing capacity increases for each proposal:

| Proposal Name | Nearby Transit Requirement | Total New Housing Units | Additional vs Pritzker |
| :--- | :--- | :--- | :--- |
| **0. Status Quo** | Currently feasible un-built multifamily lots citywide. | **{{ feasible_existing }}** | *Current Baseline* |
| **1. Original Pritzker** | Baseline "missing middle" upzoning applied evenly. | **{{ pritzker_total }}** | *Baseline* |
| **2. True CA SB 79** | Train OR intersection of 2+ high-frequency buses. | **{{ true_sb79_total }}** | **{{ true_sb79_diff }}** |
| **3. Train Only** | Strictly CTA rail stations. | **{{ train_only_total }}** | **{{ train_only_diff }}** |
| **4. Train + Bus Options** | Train AND (HF bus OR any 2 bus lines). | **{{ train_combo_total }}** | **{{ train_combo_diff }}** |
| **5. Train + HF Bus** | Train AND a 10-min frequency bus stop. | **{{ train_hf_total }}** | **{{ train_hf_diff }}** |

## The Importance of Floor Area Ratio (FAR) Near Transit

BUILD act appears to currently set FAR to 1.5. SB79 set FAR to 3.0 for projects near transit, scaling up to 4.0 with projects directly next to transit.

Our model shows that **{{ far_bump_units }}** new homes accounting for **{{ pct_far_bump }}%** of the housing unlocked in our SB 79 scenario do not require further zoning changes beyond what the BUILD Act proposes. They become financially feasible to build strictly by allowing a higher FAR (3.0 within 1/2 mile of transit).

<br>

*Use the layer toggle on the interactive map below to switch between the different transit-oriented density scenarios and see exactly how housing capacity shifts across Chicago's neighborhoods.*

*Map looks best on desktop.*
"""

    with open(config['files']['output_article_md'], 'w') as f:
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
    output_file = config['files']['output_index_html']
    with open(output_file, 'w') as f:
        f.write(final_html)

    print(f"✅ Success! Page saved to {output_file}")
