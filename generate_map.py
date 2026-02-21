import folium
from folium.features import DivIcon
import json

def build_map(df_neighborhoods):
    print("Generating Interactive Map...")
    with open('neighborhoods.geojson', 'r') as f:
        geo_data = json.load(f)

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
    return m.get_root()._repr_html_()
