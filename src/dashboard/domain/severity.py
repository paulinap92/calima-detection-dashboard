"""
Severity classification utilities for air-quality data.

This module defines a simple severity model used by the dashboard layer
to classify air-quality conditions (including Calima events) into
discrete severity levels and UI-friendly representations.

Severity levels:
    0 -> NORMAL
    1 -> SIGNATURE (moderate calima signature)
    2 -> STRONG (strong calima episode)

The logic here is intentionally lightweight and deterministic.
It is designed for visualization and high-level status reporting,
not for scientific classification.
"""

from __future__ import annotations


def compute_severity(pm10: float, pm25: float, dust: float, aod: float) -> int:
    """
    Compute air-quality severity level based on pollutant values.

    Severity rules (evaluated in order):
        - STRONG (2):
            * dust > 150
            * OR pm25 > 35 AND pm10 > 60
        - SIGNATURE (1):
            * pm10 > 50 AND aod > 0.5
        - NORMAL (0):
            * none of the above conditions met

    Args:
        pm10: PM10 concentration value.
        pm25: PM2.5 concentration value.
        dust: Dust concentration value.
        aod: Aerosol Optical Depth value.

    Returns:
        Integer severity level:
            0 = NORMAL
            1 = SIGNATURE
            2 = STRONG
    """
    if dust > 150:
        return 2
    if pm10 > 50 and aod > 0.5:
        return 1
    if pm25 > 35 and pm10 > 60:
        return 2
    return 0


def severity_label(sev: int) -> str:
    """
    Convert severity level to a human-readable label.

    Args:
        sev: Severity level (0, 1, or 2).

    Returns:
        String label corresponding to the severity:
            0 -> "NORMAL"
            1 -> "SIGNATURE"
            2 -> "STRONG"
    """
    return "STRONG" if sev == 2 else ("SIGNATURE" if sev == 1 else "NORMAL")


def severity_color(sev: int) -> list[int]:
    """
    Convert severity level to an RGB color representation.

    Colors are intended for dashboard and map visualizations.

    Args:
        sev: Severity level (0, 1, or 2).

    Returns:
        RGB color as a list of three integers:
            STRONG (2)    -> [220, 60, 60]   # red
            SIGNATURE (1) -> [255, 150, 60]  # orange
            NORMAL (0)    -> [80, 170, 120]  # green
    """
    if sev == 2:
        return [220, 60, 60]
    if sev == 1:
        return [255, 150, 60]
    return [80, 170, 120]
