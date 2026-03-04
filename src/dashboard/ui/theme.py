"""
Custom visual theme for the Calima Streamlit dashboard.

This module defines and applies a custom background theme inspired by
Calima conditions in the Canary Islands. The theme is implemented using
embedded CSS injected into the Streamlit app.

The styling:
- Applies a warm sand-like gradient background
- Hides the default Streamlit header and footer
- Uses a fixed background attachment for visual stability

This module is purely presentational and contains no business logic.
"""

import streamlit as st


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


def apply_theme() -> None:
    """
    Apply the custom Calima visual theme to the Streamlit app.

    This function injects predefined CSS styles into the Streamlit
    application using `st.markdown` with HTML enabled.

    Effects:
        - Sets a warm gradient background
        - Hides Streamlit's default header and footer
        - Improves visual consistency across dashboard pages

    Returns:
        None. The theme is applied globally to the Streamlit app.
    """
    st.markdown(CALIMA_BG, unsafe_allow_html=True)
