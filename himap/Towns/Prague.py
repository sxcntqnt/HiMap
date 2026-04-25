"""
Prague — Canary Islands dataset not applicable.
Dataset key is None until a Czech Republic dataset is registered.

Original values from Prague.py:
    x = 50.18, y = 14.22  (x=lat, y=lng — convention was inverted)
    x_growth = 0.008435, y_growth = 0.013725

NOTE: The original extents (0.008° lat, 0.013° lng) produced a viewport
of roughly 1km × 1km — very zoomed in. Updated to city-scale (0.07° × 0.12°)
which gives approximately a 15km × 17km viewport matching Prague's urban extent.
The original values are preserved below as comments if the narrow viewport
was intentional.
"""

from .TownBase import TownBase


class Prague(TownBase):
    def __init__(self):
        super().__init__(
            name="Prague",
            lat=50.0755,          # corrected from 50.18 (city center, not outskirts)
            lng=14.4378,          # corrected from 14.22
            lat_extent=0.07,      # ~15 km — city scale
            lng_extent=0.12,      # ~17 km — city scale
            dataset_key=None,     # no dataset registered yet
            country_code="CZ",
        )

        # Original narrow viewport — uncomment if point-level zoom was intentional:
        # self.lat_extent = 0.008435
        # self.lng_extent = 0.013725
