"""
Map rendering utilities for the Calima dashboard.

This module provides Streamlit + PyDeck helpers to render an interactive map
from a prepared Pandas DataFrame. The DataFrame is expected to include one
row per location with the latest measurements and visualization fields
(e.g., severity color and metric height).

The map supports:
- Scatter points (always enabled)
- Optional 3D columns (intensity extrusion)
- Optional heatmap overlay
"""

import streamlit as st
import pydeck as pdk
import pandas as pd


def render_map_pydeck(map_df: pd.DataFrame) -> None:
    """
    Render an interactive PyDeck map in Streamlit from a prepared DataFrame.

    The function expects `map_df` to contain at least:
        - name: location name
        - lat: latitude
        - lon: longitude
        - color: RGB list used by PyDeck (e.g. [80, 170, 120])
        - metric_height: numeric intensity used for 3D columns / heatmap weight
        - timestamp: formatted timestamp string
        - status: human-readable status label
        - pm10, pm25, dust, aod: numeric measurement fields

    UI controls:
        - "Visualización 3D (intensidad)": toggles ColumnLayer extrusion
        - "Mapa de intensidad (heatmap)": toggles HeatmapLayer overlay

    Behavior:
        - Always shows a ScatterplotLayer.
        - Optionally adds a ColumnLayer for 3D visualization.
        - Optionally adds a HeatmapLayer for intensity distribution.
        - Centers the view on the mean latitude/longitude in the data.

    Args:
        map_df: DataFrame containing map-ready rows per location.

    Returns:
        None. The map is rendered in the Streamlit app.
    """
    if map_df is None or map_df.empty:
        st.info("No hay datos suficientes para construir el mapa.")
        return

    c1, c2 = st.columns([1, 1])
    with c1:
        show_columns = st.toggle("Visualización 3D (intensidad)", value=True)
    with c2:
        show_heat = st.toggle("Mapa de intensidad (heatmap)", value=False)

    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position=["lon", "lat"],
        get_radius=1800,
        get_fill_color="color",
        pickable=True,
    )

    layers = [scatter]

    if show_columns:
        columns = pdk.Layer(
            "ColumnLayer",
            data=map_df,
            get_position=["lon", "lat"],
            get_elevation="metric_height",
            elevation_scale=25,
            radius=2500,
            get_fill_color="color",
            pickable=True,
            extruded=True,
        )
        layers.append(columns)

    if show_heat:
        heat = pdk.Layer(
            "HeatmapLayer",
            data=map_df,
            get_position=["lon", "lat"],
            get_weight="metric_height",
            radiusPixels=60,
        )
        layers.append(heat)

    view = pdk.ViewState(
        latitude=float(map_df["lat"].mean()),
        longitude=float(map_df["lon"].mean()),
        zoom=7.2,
        pitch=35 if show_columns else 0,
    )

    tooltip = {
        "text": (
            "{name}\n{timestamp}\n"
            "Status: {status}\n"
            "PM10: {pm10}\nPM2.5: {pm25}\n"
            "Dust: {dust}\nAOD: {aod}"
        )
    }

    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view, tooltip=tooltip))
