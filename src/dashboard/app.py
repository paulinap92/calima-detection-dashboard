"""
Streamlit dashboard entrypoint for the Calima Observatory.

This module renders the main Streamlit application page for monitoring
air-quality indicators in the Canary Islands using:
- MongoDB (stored historical hourly measurements)
- Open-Meteo (updates + forecast returned to the UI)
- Rule-based Calima detection (events stored in MongoDB)

The dashboard includes:
- Current-status map (PyDeck)
- Location selector
- Latest measurement summary (metrics)
- Quick severity summary
- Hourly time-series charts (real + optional forecast)
- Daily averages chart
- List of detected Calima episodes
"""

import streamlit as st
import plotly.graph_objects as go

from src.dashboard.ui.theme import apply_theme
from src.dashboard.ui.legend import legend_block
from src.dashboard.ui.map import render_map_pydeck
from src.dashboard.ui.charts import add_event_vrects

from src.dashboard.data.db import (
    connect_db,
    safe_disconnect,
    make_repo,
    make_updater,
    load_locations,
    load_measurements,
    load_daily,
    load_events,
    build_map_df,
)

from src.dashboard.domain.severity import compute_severity, severity_label


def main() -> None:
    st.set_page_config(
        page_title="Calima – Observatorio Canarias",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_theme()

    connect_db()
    repo = make_repo()
    updater = make_updater()

    try:
        st.title("🌫️ Observatorio de Calima – Islas Canarias")
        st.caption("Monitorización de PM10, PM2.5, Dust y AOD (MongoDB + Open-Meteo)")

        st.sidebar.header("⚙️ Configuración")
        st.sidebar.info("📡 Datos actualizados automáticamente cada hora")

        locations = load_locations()
        if not locations:
            st.error("⚠️ No hay localizaciones en la base de datos.")
            return

        loc_names_sorted = sorted([loc.name for loc in locations])

        if "chosen_location" not in st.session_state:
            st.session_state["chosen_location"] = loc_names_sorted[0]

        # ---------------------------------------------------------
        # MAPA – ESTADO ACTUAL
        # ---------------------------------------------------------
        st.subheader("🗺️ Mapa – estado actual (detección basada en reglas)")
        map_df = build_map_df(locations, repo)
        if map_df.empty:
            st.info("No hay datos suficientes para construir el mapa.")
        else:
            render_map_pydeck(map_df)
            legend_block()

        # ---------------------------------------------------------
        # SELECTOR DE LOCALIZACIÓN
        # ---------------------------------------------------------
        st.markdown("### 📍 Seleccionar localización (orden alfabético)")
        current = st.session_state["chosen_location"]
        idx = loc_names_sorted.index(current) if current in loc_names_sorted else 0

        chosen = st.selectbox(
            "Seleccionar localización:",
            options=loc_names_sorted,
            index=idx,
            key="chosen_location_select",
        )
        st.session_state["chosen_location"] = chosen

        st.divider()

        # ---------------------------------------------------------
        # DATOS DESDE DB
        # ---------------------------------------------------------
        measurements = load_measurements(repo, chosen)
        daily = load_daily(repo, chosen)
        events = load_events(repo, chosen)

        if not measurements:
            st.warning(
                "No hay datos almacenados todavía. "
                "Espera a la primera actualización automática."
            )
            return

        # Forecast (no persistido en MongoDB)
        try:
            _, forecast = updater.fetch_latest_update(chosen)
        except Exception:
            forecast = None

        # ---------------------------------------------------------
        # ÚLTIMA MEDICIÓN
        # ---------------------------------------------------------
        st.subheader("📍 Última medición registrada")
        last = measurements[-1]
        timestamp_str = last.data.timestamp.strftime("%Y-%m-%d %H:%M")
        st.markdown(f"**Última actualización:** `{timestamp_str}`")

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("PM10", last.data.pm10)
        col2.metric("PM2.5", last.data.pm25)
        col3.metric("Dust", last.data.dust)
        col4.metric("AOD", last.data.aod)
        col5.metric("Calima", "🔥 SÍ" if last.data.is_calima else "– NO")

        st.divider()

        # ---------------------------------------------------------
        # RESUMEN RÁPIDO
        # ---------------------------------------------------------
        st.subheader("🧠 Resumen rápido")

        pm10_now = float(last.data.pm10 or 0)
        pm25_now = float(last.data.pm25 or 0)
        dust_now = float(last.data.dust or 0)
        aod_now = float(last.data.aod or 0)
        sev_now = compute_severity(pm10_now, pm25_now, dust_now, aod_now)

        left, right = st.columns([2, 3])
        with left:
            st.markdown(f"**Estado actual:** `{severity_label(sev_now)}`")
            st.markdown(f"**Localización:** `{chosen}`")
            st.markdown(f"**Última actualización:** `{timestamp_str}`")

        with right:
            if events:
                last_event = events[-1]
                st.markdown("**Último episodio detectado:**")
                st.markdown(
                    f"- ⏱️ `{last_event.start_time}` → `{last_event.end_time}`\n"
                    f"- 🟠 Pico PM10: `{last_event.peak_pm10}`\n"
                    f"- 🌫️ Pico Dust: `{last_event.peak_dust}`\n"
                    f"- 🌁 Pico AOD: `{last_event.peak_aod}`"
                )
                st.caption(f"Episodios detectados en esta localización: {len(events)}")
            else:
                st.info("No se han detectado episodios de calima para esta localización.")

        st.divider()

        # ---------------------------------------------------------
        # GRÁFICO HORARIO
        # ---------------------------------------------------------
        st.subheader("📈 Calidad del aire – valores horarios")

        df_hourly = {
            "time": [m.data.timestamp for m in measurements],
            "pm10": [m.data.pm10 for m in measurements],
            "pm25": [m.data.pm25 for m in measurements],
            "dust": [m.data.dust for m in measurements],
            "aod": [m.data.aod for m in measurements],
        }

        fig = go.Figure()
        add_event_vrects(fig, events)

        fig.add_trace(go.Scatter(x=df_hourly["time"], y=df_hourly["pm10"], mode="lines", name="PM10 (real)", line=dict(width=3)))
        fig.add_trace(go.Scatter(x=df_hourly["time"], y=df_hourly["pm25"], mode="lines", name="PM2.5 (real)", line=dict(width=3)))
        fig.add_trace(go.Scatter(x=df_hourly["time"], y=df_hourly["dust"], mode="lines", name="Dust (real)", line=dict(width=3)))
        fig.add_trace(go.Scatter(x=df_hourly["time"], y=df_hourly["aod"], mode="lines", name="AOD (real)", line=dict(width=4), yaxis="y2"))

        if forecast:
            forecast_df = {
                "time": [f.timestamp for f in forecast],
                "pm10": [f.pm10 for f in forecast],
                "pm25": [f.pm25 for f in forecast],
                "dust": [f.dust for f in forecast],
                "aod": [f.aod for f in forecast],
            }

            fig.add_trace(go.Scatter(x=forecast_df["time"], y=forecast_df["pm10"], mode="lines", name="PM10 (predicción)", line=dict(width=2, dash="dash")))
            fig.add_trace(go.Scatter(x=forecast_df["time"], y=forecast_df["pm25"], mode="lines", name="PM2.5 (predicción)", line=dict(width=2, dash="dash")))
            fig.add_trace(go.Scatter(x=forecast_df["time"], y=forecast_df["dust"], mode="lines", name="Dust (predicción)", line=dict(width=2, dash="dash")))
            fig.add_trace(go.Scatter(x=forecast_df["time"], y=forecast_df["aod"], mode="lines", name="AOD (predicción)", line=dict(width=3, dash="dot"), yaxis="y2"))

        fig.update_layout(
            template="plotly_white",
            height=440,
            title=f"Valores horarios – {chosen}",
            xaxis=dict(title="Tiempo"),
            yaxis=dict(title="PM10 / PM2.5 / Dust (µg/m³)"),
            yaxis2=dict(title="AOD", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ---------------------------------------------------------
        # PROMEDIOS DIARIOS
        # ---------------------------------------------------------
        st.subheader("📊 Promedios diarios")
        if daily:
            df_daily = {
                "day": [d["_id"] for d in daily],
                "pm10": [d["pm10"] for d in daily],
                "pm25": [d["pm25"] for d in daily],
                "dust": [d["dust"] for d in daily],
                "aod": [d["aod"] for d in daily],
            }

            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["pm10"], mode="lines", name="PM10", line=dict(width=3)))
            fig2.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["pm25"], mode="lines", name="PM2.5", line=dict(width=3)))
            fig2.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["dust"], mode="lines", name="Dust", line=dict(width=3)))
            fig2.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["aod"], mode="lines", name="AOD", line=dict(width=4), yaxis="y2"))

            fig2.update_layout(
                template="plotly_white",
                height=380,
                title=f"Promedios diarios – {chosen}",
                xaxis=dict(title="Día"),
                yaxis=dict(title="PM10 / PM2.5 / Dust (µg/m³)"),
                yaxis2=dict(title="AOD", overlaying="y", side="right"),
                legend=dict(orientation="h"),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No hay promedios diarios disponibles.")

        st.divider()

        # ---------------------------------------------------------
        # EPISODIOS DE CALIMA
        # ---------------------------------------------------------
        st.subheader("🔥 Episodios de Calima")
        if events:
            for e in events:
                st.warning(
                    f"🟠 {e.start_time} → {e.end_time} | "
                    f"PM10 pico={e.peak_pm10} | Dust pico={e.peak_dust} | AOD pico={e.peak_aod}"
                )
        else:
            st.info("No se han detectado episodios de calima.")

    finally:
        safe_disconnect()
