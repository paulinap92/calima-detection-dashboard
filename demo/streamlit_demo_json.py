import json
from pathlib import Path

import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.graph_objects as go


# ==========================================================
# PAGE CONFIG
# ==========================================================
st.set_page_config(
    page_title="Calima – Observatorio Canarias (DEMO JSON)",
    layout="wide",
    initial_sidebar_state="expanded",
)

CALIMA_BG = """
<style>
    body { background-color: #f3e6c7 !important; }
    .stApp {
        background: linear-gradient(
            180deg,
            rgba(243, 230, 199, 1) 0%,
            rgba(236, 211, 170, 1) 40%,
            rgba(229, 197, 157, 1) 100%
        ) !important;
        background-attachment: fixed;
    }
    header, footer {visibility: hidden;}
</style>
"""
st.markdown(CALIMA_BG, unsafe_allow_html=True)

DEFAULT_DATA_PATH = Path("calima_export.json")


# ==========================================================
# RULES / UI HELPERS
# ==========================================================
def severity_from_row(pm10: float, pm25: float, dust: float, aod: float) -> int:
    if dust > 150:
        return 2
    if pm10 > 50 and aod > 0.5:
        return 1
    if pm25 > 35 and pm10 > 60:
        return 2
    return 0


def severity_label(sev: int) -> str:
    return "STRONG" if sev == 2 else ("SIGNATURE" if sev == 1 else "NORMAL")


def severity_color(sev: int) -> list[int]:
    if sev == 2:
        return [220, 60, 60]
    if sev == 1:
        return [255, 150, 60]
    return [80, 170, 120]


def legend_html() -> str:
    return """
<div style="
    display:flex;
    gap:18px;
    align-items:center;
    padding:10px 12px;
    background: rgba(255,255,255,0.55);
    border: 1px solid rgba(0,0,0,0.08);
    border-radius: 10px;
    width: fit-content;
">
  <div style="display:flex; align-items:center; gap:8px;">
    <span style="width:12px; height:12px; border-radius:50%; background:#50AA78; display:inline-block;"></span>
    <span style="font-size:14px;"><b>NORMAL</b></span>
  </div>
  <div style="display:flex; align-items:center; gap:8px;">
    <span style="width:12px; height:12px; border-radius:50%; background:#FF963C; display:inline-block;"></span>
    <span style="font-size:14px;"><b>SIGNATURE</b> (PM10&gt;50 &amp; AOD&gt;0.5)</span>
  </div>
  <div style="display:flex; align-items:center; gap:8px;">
    <span style="width:12px; height:12px; border-radius:50%; background:#DC3C3C; display:inline-block;"></span>
    <span style="font-size:14px;"><b>STRONG</b> (Dust&gt;150 lub PM2.5&gt;35 &amp; PM10&gt;60)</span>
  </div>
</div>
"""


# ==========================================================
# DATA LOAD
# ==========================================================
@st.cache_data
def load_payload(path_str: str) -> dict:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


@st.cache_data
def to_frames(payload: dict):
    locations = payload.get("locations", []) or []
    measurements = payload.get("measurements", []) or []
    events = payload.get("events", []) or []

    loc_df = pd.DataFrame(locations)
    meas_df = pd.DataFrame(measurements)
    ev_df = pd.DataFrame(events)

    # --- LOCATIONS
    if not loc_df.empty:
        if "lat" in loc_df.columns and "latitude" not in loc_df.columns:
            loc_df["latitude"] = loc_df["lat"]
        if "lon" in loc_df.columns and "longitude" not in loc_df.columns:
            loc_df["longitude"] = loc_df["lon"]

        for c in ["latitude", "longitude"]:
            if c in loc_df.columns:
                loc_df[c] = pd.to_numeric(loc_df[c], errors="coerce")

        if "name" not in loc_df.columns:
            loc_df["name"] = None

    # --- MEASUREMENTS
    if not meas_df.empty:
        if "timestamp" not in meas_df.columns:
            if "datetime" in meas_df.columns:
                meas_df["timestamp"] = meas_df["datetime"]
            else:
                meas_df["timestamp"] = None

        meas_df["timestamp"] = pd.to_datetime(meas_df["timestamp"], errors="coerce")

        for c in ["pm10", "pm25", "dust", "aod"]:
            if c not in meas_df.columns:
                meas_df[c] = 0
            meas_df[c] = pd.to_numeric(meas_df[c], errors="coerce").fillna(0)

        if "location" not in meas_df.columns:
            meas_df["location"] = "unknown"

        meas_df["severity"] = meas_df.apply(
            lambda r: severity_from_row(float(r["pm10"]), float(r["pm25"]), float(r["dust"]), float(r["aod"])),
            axis=1,
        )
        meas_df["status"] = meas_df["severity"].apply(severity_label)
        meas_df["color"] = meas_df["severity"].apply(severity_color)
        meas_df["metric_height"] = meas_df.apply(
            lambda r: float(r["dust"]) if float(r["dust"]) > 0 else float(r["pm10"]),
            axis=1,
        )

        meas_df = meas_df.dropna(subset=["timestamp"]).sort_values("timestamp")

    # --- EVENTS
    if not ev_df.empty:
        for c in ["start_time", "end_time"]:
            if c not in ev_df.columns:
                ev_df[c] = None
            ev_df[c] = pd.to_datetime(ev_df[c], errors="coerce")

        if "location" not in ev_df.columns:
            ev_df["location"] = "unknown"

    return loc_df, meas_df, ev_df


def add_event_vrects(fig, events_df: pd.DataFrame):
    if events_df is None or events_df.empty:
        return
    events_df = events_df.dropna(subset=["start_time", "end_time"])
    for _, e in events_df.iterrows():
        fig.add_vrect(
            x0=e["start_time"],
            x1=e["end_time"],
            fillcolor="rgba(255, 122, 0, 0.15)",
            line_width=0,
            layer="below",
        )


# ==========================================================
# APP
# ==========================================================
def main():
    st.title("🌫️ Observatorio de Calima – Islas Canarias (DEMO)")
    st.caption("Offline demo z pliku JSON (export z Twojej bazy)")

    # ---------------- Sidebar ----------------
    st.sidebar.header("⚙️ Configuración")
    data_path = st.sidebar.text_input("Ruta do JSON:", value=str(DEFAULT_DATA_PATH))

    try:
        payload = load_payload(data_path)
    except Exception as e:
        st.error(f"Nie mogę wczytać JSON: {e}")
        return

    meta = payload.get("meta", {}) or {}
    exported_at = meta.get("exported_at", "unknown")
    days_back = meta.get("days_back", None)

    caption = f"Export: {exported_at}"
    if days_back is not None:
        caption += f" | days_back={days_back}"
    st.sidebar.caption(caption)

    loc_df, meas_df, ev_df = to_frames(payload)

    if meas_df.empty:
        st.error("Brak danych w measurements w JSON (albo timestamps są puste).")
        return

    # lista lokalizacji (alfabetycznie)
    locations = sorted(meas_df["location"].dropna().unique().tolist())
    if not locations:
        st.error("Brak lokalizacji w danych.")
        return

    # session state dla wyboru
    if "chosen_location" not in st.session_state:
        st.session_state["chosen_location"] = locations[0]

    # ================= MAP (PYDECK) =================
    st.subheader("🗺️ Mapa – estado actual (rule-based)")

    # last measurement per location
    last_idx = meas_df.groupby("location")["timestamp"].idxmax()
    last_df = meas_df.loc[last_idx].copy()

    # dołącz coords
    if not loc_df.empty and "name" in loc_df.columns:
        last_df = last_df.merge(loc_df, left_on="location", right_on="name", how="left")

    # filtrowanie, żeby pydeck się nie wysypał
    if "latitude" in last_df.columns and "longitude" in last_df.columns:
        last_df = last_df.dropna(subset=["latitude", "longitude"])
    else:
        last_df = pd.DataFrame()

    if last_df.empty:
        st.info("Mapa: brak współrzędnych w JSON (locations) albo nie pasują nazwy location↔name.")
    else:
        c1, c2 = st.columns([1, 1])
        with c1:
            show_columns = st.toggle("Visualización 3D (intensidad)", value=True)
        with c2:
            show_heat = st.toggle("Mapa de intensidad (heatmap)", value=False)

        scatter = pdk.Layer(
            "ScatterplotLayer",
            data=last_df,
            get_position=["longitude", "latitude"],
            get_radius=1800,
            get_fill_color="color",
            pickable=True,
        )

        layers = [scatter]

        if show_columns:
            columns = pdk.Layer(
                "ColumnLayer",
                data=last_df,
                get_position=["longitude", "latitude"],
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
                data=last_df,
                get_position=["longitude", "latitude"],
                get_weight="metric_height",
                radiusPixels=60,
            )
            layers.append(heat)

        view = pdk.ViewState(
            latitude=float(last_df["latitude"].mean()),
            longitude=float(last_df["longitude"].mean()),
            zoom=7.2,
            pitch=35 if show_columns else 0,
        )

        tooltip = {
            "text": (
                "{location}\n{timestamp}\n"
                "Status: {status}\n"
                "PM10: {pm10}\nPM2.5: {pm25}\n"
                "Dust: {dust}\nAOD: {aod}"
            )
        }

        st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view, tooltip=tooltip))
        st.markdown(legend_html(), unsafe_allow_html=True)

    # ================= LOCATION PICKER UNDER MAP =================
    st.markdown("### 📍 Wybierz lokalizację (alfabetycznie)")
    st.caption("Lista jest pod mapą i działa stabilnie nawet przy dużej liczbie punktów.")

    # index aktualnego wyboru
    cur = st.session_state["chosen_location"]
    idx = locations.index(cur) if cur in locations else 0

    chosen = st.selectbox(
        "Seleccionar localización:",
        options=locations,
        index=idx,
        key="chosen_location_select",
    )
    st.session_state["chosen_location"] = chosen

    st.divider()

    # ================= LOCATION VIEW =================
    loc_meas = meas_df[meas_df["location"] == chosen].sort_values("timestamp")
    loc_events = ev_df[ev_df["location"] == chosen].sort_values("start_time") if not ev_df.empty else pd.DataFrame()

    if loc_meas.empty:
        st.warning("Brak danych dla wybranej lokalizacji.")
        return

    last = loc_meas.iloc[-1]
    ts_str = last["timestamp"].strftime("%Y-%m-%d %H:%M")

    st.subheader("📍 Última medición registrada")
    st.markdown(f"**Última actualización:** `{ts_str}`")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("PM10", float(last["pm10"]))
    c2.metric("PM2.5", float(last["pm25"]))
    c3.metric("Dust", float(last["dust"]))
    c4.metric("AOD", float(last["aod"]))
    c5.metric("Estado", severity_label(int(last["severity"])))

    st.divider()

    st.subheader("📈 Calidad del aire – Valores horarios")
    fig = go.Figure()
    add_event_vrects(fig, loc_events)

    fig.add_trace(go.Scatter(x=loc_meas["timestamp"], y=loc_meas["pm10"], mode="lines", name="PM10", line=dict(width=3)))
    fig.add_trace(go.Scatter(x=loc_meas["timestamp"], y=loc_meas["pm25"], mode="lines", name="PM2.5", line=dict(width=3)))
    fig.add_trace(go.Scatter(x=loc_meas["timestamp"], y=loc_meas["dust"], mode="lines", name="Dust", line=dict(width=3)))
    fig.add_trace(go.Scatter(x=loc_meas["timestamp"], y=loc_meas["aod"], mode="lines", name="AOD", line=dict(width=4), yaxis="y2"))

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

    st.subheader("🔥 Episodios de Calima")
    if loc_events is not None and not loc_events.empty:
        for _, e in loc_events.dropna(subset=["start_time", "end_time"]).iterrows():
            st.warning(
                f"🟠 {e['start_time']} → {e['end_time']} | "
                f"PM10 pico={e.get('peak_pm10', None)} | Dust pico={e.get('peak_dust', None)} | AOD pico={e.get('peak_aod', None)}"
            )
    else:
        st.info("No se han detectado episodios de calima.")


if __name__ == "__main__":
    main()
