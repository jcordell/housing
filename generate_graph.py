import duckdb
import pandas as pd
import folium
from folium.features import DivIcon
import json
import webbrowser
import os

DB_FILE = "chicago_housing.duckdb"
OUTPUT_MAP = "chicago_housing_clean_map.html"

def analyze_and_map():
    if not os.path.exists('neighborhoods.geojson'):
        print("ERROR: 'neighborhoods.geojson' missing. Run 'download.py' first.")
        return

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL spatial; LOAD spatial;")

    print("Running spatial analysis...")

    # ---------------------------------------------------------
    # SQL QUERY (Same robust logic)
    # ---------------------------------------------------------
    query = """
    WITH
    target_zones AS (
        SELECT
            ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,
            zone_class
        FROM zoning
        WHERE zone_class IN ('RS-1', 'RS-2', 'RS-3')
    ),
    processed_parcels AS (
        SELECT
            pin10,
            ST_Transform(geom, 'EPSG:4326', 'EPSG:3435', true) as geom_3435,
            ST_Centroid(geom) as center_geom
        FROM parcels
        WHERE geom IS NOT NULL
    ),
    parcel_calculations AS (
        SELECT
            p.center_geom,
            ST_Area(p.geom_3435) as area_sqft,
            1 as current_units,
            CASE
                WHEN ST_Area(p.geom_3435) < 2500 THEN 1
                WHEN ST_Area(p.geom_3435) >= 2500 AND ST_Area(p.geom_3435) < 5000 THEN 4
                WHEN ST_Area(p.geom_3435) >= 5000 AND ST_Area(p.geom_3435) < 7500 THEN 6
                WHEN ST_Area(p.geom_3435) >= 7500 THEN 8
                ELSE 1
            END as proposed_units
        FROM processed_parcels p, target_zones z
        WHERE ST_Intersects(p.geom_3435, z.geom_3435)
    ),
    neighborhood_stats AS (
        SELECT
            n.community as neighborhood_name,
            SUM(pc.proposed_units - pc.current_units) as total_new_units,
            ST_Y(ST_Centroid(n.geom)) as label_lat,
            ST_X(ST_Centroid(n.geom)) as label_lon
        FROM parcel_calculations pc
        JOIN ST_Read('neighborhoods.geojson') n ON ST_Intersects(pc.center_geom, n.geom)
        WHERE (pc.proposed_units - pc.current_units) > 0
        GROUP BY n.community, n.geom
    )
    SELECT * FROM neighborhood_stats ORDER BY total_new_units DESC;
    """

    try:
        df = con.execute(query).df()
    except Exception as e:
        print(f"Query Error: {e}")
        con.close()
        return

    con.close()

    if df.empty:
        print("No data found.")
        return

    print(f"Total New Units: {df['total_new_units'].sum():,.0f}")

    # ---------------------------------------------------------
    # MAPPING
    # ---------------------------------------------------------
    print("Generating Clean Map...")

    with open('neighborhoods.geojson', 'r') as f:
        geo_data = json.load(f)

    df['neighborhood_name'] = df['neighborhood_name'].str.upper()

    # 1. Base Map (Dark Matter looks best with Green scale)
    m = folium.Map(location=[41.84, -87.68], zoom_start=11, tiles="CartoDB dark_matter")

    # 2. Choropleth Layer (Green Scale)
    folium.Choropleth(
        geo_data=geo_data,
        name="Housing Potential",
        data=df,
        columns=['neighborhood_name', 'total_new_units'],
        key_on='feature.properties.community',
        fill_color='Greens',
        fill_opacity=0.7,
        line_opacity=0.2,
        line_color='white',
        legend_name='Potential New Units'
    ).add_to(m)

    # 3. Clean Text Labels
    for i, row in df.iterrows():
        units = row['total_new_units']
        lat = row['label_lat']
        lon = row['label_lon']

        # Format: "24k"
        if units >= 1000:
            label_text = f"{int(round(units/1000))}k"
        elif units > 0:
            label_text = "<1k"
        else:
            continue

        # HTML Style: No background, just text with shadow
        label_html = f'''
            <div style="
                font-family: sans-serif;
                font-size: 8pt;             /* Smaller font size */
                color: white;               /* White text */
                text-shadow: 1px 1px 2px black; /* Black shadow for readability */
                text-align: center;
                white-space: nowrap;
                transform: translate(-50%, -50%); /* Center exactly on the point */
                pointer-events: none;       /* Allow clicking through the text */
            ">
            {label_text}
            </div>
        '''

        folium.map.Marker(
            [lat, lon],
            icon=DivIcon(
                icon_size=(50,20),
                icon_anchor=(0,0),
                html=label_html
            )
        ).add_to(m)

    # 4. Tooltips
    unit_lookup = df.set_index('neighborhood_name')['total_new_units'].to_dict()

    for feature in geo_data['features']:
        name = feature['properties']['community'].upper()
        count = unit_lookup.get(name, 0)
        feature['properties']['new_units_display'] = f"{count:,.0f}"

    folium.GeoJson(
        geo_data,
        style_function=lambda x: {'fillColor': '#ffffff', 'color':'transparent', 'fillOpacity': 0.0},
        highlight_function=lambda x: {'fillColor': '#ffffff', 'color':'white', 'fillOpacity': 0.2, 'weight': 2},
        tooltip=folium.GeoJsonTooltip(
            fields=['community', 'new_units_display'],
            aliases=['Neighborhood:', 'Total Potential Units:'],
            style="background-color: black; color: white; border-radius: 5px; font-family: sans-serif;"
        )
    ).add_to(m)

    m.save(OUTPUT_MAP)
    print(f"Map saved to {OUTPUT_MAP}")

    try:
        webbrowser.open('file://' + os.path.realpath(OUTPUT_MAP))
    except:
        pass

if __name__ == "__main__":
    analyze_and_map()
