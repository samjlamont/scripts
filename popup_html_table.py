def popup_html(df):
    city_id = df['city_id'].iloc[0]
    city_name = df['city_name'].iloc[0]
    event_id = df['event_id'].iloc[0]
    rmse = df['rmse'].iloc[0]
    mae = df['mean_absolute_error'].iloc[0]
    mean_error = df['mean_error'].iloc[0]
    r_squared = df['r_squared'].iloc[0]
    nse = df['nse'].iloc[0]
    hit_rate = df['hit_rate_gsi'].iloc[0]

    left_col_color = "#19a7bd"
    right_col_color = "#f2f0d3"

    html = """<!DOCTYPE html>
                <html>

                <head>
                <h4 style="margin-bottom:10"; width="200px">{}</h4>""".format(city_name) + """

                </head>
                    <table style="height: 126px; width: 350px;">
                <tbody>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">City ID</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(city_id) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">Event ID</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(event_id) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">RMSE</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(rmse) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">MAE</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(mae) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">Mean Error</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(mean_error) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">r-squared</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(r_squared) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">NSE</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(nse) + """
                </tr>
                <tr>
                <td style="background-color: """ + left_col_color + """;"><span style="color: #ffffff;">Hit Rate</span></td>
                <td style="width: 150px;background-color: """ + right_col_color + """;">{}</td>""".format(hit_rate) + """
                </tr>
                </tbody>
                </table>
                </html>
                """
    return html


def build_gsi_folium_map(
    gdf_this_geo: gpd.GeoDataFrame,
    sim_depth_total: np.array,
    sim_kdtree: cKDTree,
    gsi_filepath: str,
    df_metrics: pd.DataFrame,
) -> folium.Map:

    print("\tBuilding GSI vs. SCHISM extent map")

    # Get attributes of the geo boundary
    xmin, ymin, xmax, ymax = gdf_this_geo.total_bounds
    start_lon = gdf_this_geo.geometry.centroid.x.values[0]
    start_lat = gdf_this_geo.geometry.centroid.y.values[0]

    # Establish map
    m_gsi = folium.Map(
        [start_lat, start_lon], zoom_start=12, tiles="cartodbpositron"
    )  # tiles="cartodbpositron" "Stamen Terrain"
    # Also add satellite basemap
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Esri Satellite",
        overlay=False,
        control=True,
    ).add_to(m_gsi)

    # Create feature groups for layer control
    fg_geo = folium.FeatureGroup(name="GEO boundary")
    fg_schism = folium.FeatureGroup(name="SCHISM max flood extent")
    fg_gsi = folium.FeatureGroup(name="GSI flood exent")

    # Get max schism depth to a regular 2D grid using nearest-neighbor for the image overlay
    res = 0.0003  # ~30m, for mapping SCHISM output to a regular grid
    arr_2d = schism_xyz_to_regular_grid(
        xmin, ymin, xmax, ymax, res, sim_depth_total, sim_kdtree,
    )
    arr_2d[arr_2d <= DEPTH_THRESHOLD] = np.nan

    # print("\tAdding SCHISM extent map")
    fg_schism.add_child(
        folium.raster_layers.ImageOverlay(
            image=arr_2d,
            bounds=[[ymin, xmin], [ymax, xmax]],
            colormap=plt.cm.Spectral_r,
            opacity=0.9,
        )
    )
    # print("\tAdding GSI extent map")
    # Add GSI map clipped to geo boundary
    style = {"color": "black", "weight": "0.5", "fill": True, "fillOpacity": 0.75, "fillColor": "black"}
    gdf_gsi = gpd.read_file(gsi_filepath)
    fg_gsi.add_child(
        folium.GeoJson(
            data=gdf_gsi.geometry.to_json(), style_function=lambda x:style
        )
    )

    city_name = gdf_this_geo.iloc[0]["E-Name"]
    city_id = gdf_this_geo.iloc[0]["ID"]
    fg_geo.add_child(
        folium.GeoJson(
            data=gdf_this_geo.geometry.iloc[0], tooltip=f"{city_name}, ID:{city_id}"
        )
    )

    # # TEST colorbar for the image overlay
    # import branca.colormap as brcm
    # lst_cmap = ["blue", "green", "yellow", "orange", "red"]
    # max_val = int(arr_2d[~np.isnan(arr_2d)].max())
    # colormap = brcm.LinearColormap(colors=lst_cmap, vmin=DEPTH_THRESHOLD, vmax=max_val, caption="SCHISM depth (m)")

    # m.add_child(colormap)
    m_gsi.add_child(fg_geo)
    m_gsi.add_child(fg_schism)
    m_gsi.add_child(fg_gsi)
    m_gsi.add_child(folium.LayerControl())

    return m_gsi


# Add summary metrics table as html
html_table = popup_html(df_metrics)
iframe = folium.IFrame(html_table, width=300, height=400)
popup = folium.Popup(iframe, max_width=400)
marker = folium.Marker([xmin + 0.05, ymax-0.05], popup=popup, tooltip="click for summary error metrics").add_to(m_gsi)
m_gsi.add_child(marker)






fg_gsi.add_child(folium.raster_layers.ImageOverlay(colored_data,bounds=[[ymin, xmin], [ymax, xmax]],opacity=0.7))
