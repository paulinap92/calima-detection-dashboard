"""
Plotly helpers for visualizing Calima events.

This module contains small utility functions used by the dashboard
to enhance Plotly figures with domain-specific visual elements.

Currently supported:
- Vertical shaded regions (vrects) representing detected Calima events
"""

import plotly.graph_objects as go


def add_event_vrects(fig: go.Figure, events) -> None:
    """
    Add vertical shaded regions to a Plotly figure for Calima events.

    Each event is rendered as a semi-transparent vertical rectangle
    spanning from the event's start time to its end time.

    The rectangles are drawn below the data traces so that
    charts remain readable.

    Args:
        fig: Plotly Figure object to which the event regions will be added.
        events: Iterable of CalimaEvent-like objects. Each object is expected
            to expose `start_time` and `end_time` datetime attributes.

    Returns:
        None. The figure is modified in place.
    """
    if not events:
        return

    for e in events:
        fig.add_vrect(
            x0=e.start_time,
            x1=e.end_time,
            fillcolor="rgba(255, 122, 0, 0.15)",
            line_width=0,
            layer="below",
        )
