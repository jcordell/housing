# Proposal for a TOD Amendment to the BUILD Act

Illinois is facing a severe housing shortage. Governor Pritzker’s proposed BUILD Act is a critical first step, unlocking "missing middle" housing by allowing multi-unit developments on historically restricted single-family lots. Our analysis shows the base BUILD Act could unlock **{{ pritzker_total }}** new housing units across Chicago.

This proposal analyzes adding a Transit-Oriented Development (TOD) amendment similar to California's recently passed SB79.

## The "Missing Middle" in High-Cost Areas
We analyzed the Zillow Observed Rent Index (ZORI) to identify Chicago's top 15 neighborhoods experiencing the most extreme rent spikes. Under the base BUILD Act, only **{{ pct_pritzker }}%** of new citywide housing capacity falls within these critical high-cost areas, despite them comprising **{{ pct_top15_area }}%** of the city's residential land.

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

## Methodology: Defining "Feasible" Redevelopment
To ensure our model reflects real-world market conditions rather than purely theoretical maximums, a parcel is only counted as a "feasible un-built multifamily lot" if it passes a strict set of physical, legal, and economic filters:

* **Zoning & Property Type:** The parcel must be zoned for residential or commercial/mixed-use (excluding parks and manufacturing). It excludes existing condominiums, tax-exempt properties, and anomalies like "sliver" lots (assessed under $1,000).
* **Age & Preservation:** The existing structure must be at least 35 years old or an empty/low-value lot. It must also have fewer than 40 existing units to prevent modeling the demolition of large, currently viable apartment complexes.
* **Meaningful Density Increase:** The proposed development must at least double the existing unit count and increase total building square footage by at least 25%. The existing building must not already be hyper-dense (existing Floor Area Ratio < 1.5), and the lot must be 1 acre or smaller.
* **Financial Viability (Pro Forma ROI):** This is the most rigorous constraint. A parcel is only feasible if the projected revenue exceeds total project costs by at least a 15% profit margin. The pro forma components are calculated as follows:
    * **Projected Revenue:** Calculated dynamically using recent new-build condo sale prices specific to that exact neighborhood, multiplied by the legally allowed number of new units.
    * **Acquisition Cost:** The Cook County Assessor's estimated market value of the existing property, adjusted upwards by a neighborhood-specific multiplier based on recent real estate sales data to reflect true market rate.
    * **Construction Cost:** A graduated hard cost scale per unit reflecting real-world labor and material expenses (e.g., mid-sized and larger buildings trigger higher per-unit costs).

## Transit Proximity Policy Options
We analyzed four different legislative requirements for triggering transit-based upzoning. We compared the base SB79 text (upzoning units near Trains OR Bus Intersections) to alternatives requiring varying levels of access to transportation.

*(Note: Data filtered for financial feasibility. Parcels are only counted if the zoning allows a significant multiplier over existing capacity).*

We calculated the following housing capacity increases for each proposal:

| Proposal Name | Nearby Transit Requirement | Total New Housing Units | Additional vs Pritzker |
| :--- | :--- | :--- | :--- |
| **0. Status Quo** | Currently feasible un-built multifamily lots citywide. | **{{ feasible_existing }}** | *Current Baseline* |
| **1. Original Pritzker** | Baseline "missing middle" upzoning applied evenly. | **{{ pritzker_total }}** | *Baseline* |
| **2. True CA SB 79** | Train OR intersection of 2+ high-frequency buses. | **{{ true_sb79_total }}** | **{{ true_sb79_diff }}** |
| **3. Train Only** | Strictly CTA/Metra rail stations. | **{{ train_only_total }}** | **{{ train_only_diff }}** |
| **4. Train + Bus Options** | Train AND (HF bus OR any 2 bus lines). | **{{ train_combo_total }}** | **{{ train_combo_diff }}** |
| **5. Train + HF Bus** | Train AND a 10-min frequency bus stop. | **{{ train_hf_total }}** | **{{ train_hf_diff }}** |

<br>

*Use the layer toggle on the interactive map below to switch between the different transit-oriented density scenarios and see exactly how housing capacity shifts across Chicago's neighborhoods.*

*Map looks best on desktop.*
