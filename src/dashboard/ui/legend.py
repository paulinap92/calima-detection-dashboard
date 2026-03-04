"""
Streamlit UI components for the Calima dashboard.

This module contains small reusable UI blocks rendered with Streamlit.
The components here are purely presentational and do not contain
any business or domain logic.

Currently included:
- Severity legend block for air-quality / Calima classification
"""

import streamlit as st


def legend_block() -> None:
    """
    Render the air-quality severity legend.

    This function displays a styled HTML legend explaining the meaning
    of severity levels used across the dashboard.

    Severity levels:
        - NORMAL: background air quality
        - SIGNATURE: moderate Calima signature
          (PM10 > 50 AND AOD > 0.5)
        - STRONG: strong Calima episode
          (Dust > 150 OR PM2.5 > 35 AND PM10 > 60)

    The legend is rendered as a horizontal block with color-coded dots
    matching the colors used in maps and charts.

    Notes:
        - Uses `st.markdown` with `unsafe_allow_html=True`.
        - Intended to be placed near maps or time-series charts.
        - Purely visual; no return value or side effects beyond rendering.
    """
    st.markdown(
        """
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
            <span style="font-size:14px;"><b>STRONG</b> (Dust&gt;150 o PM2.5&gt;35 &amp; PM10&gt;60)</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
