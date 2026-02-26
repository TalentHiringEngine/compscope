"""
BLS OEWS Client
===============
Queries the Bureau of Labor Statistics Occupational Employment and Wage
Statistics (OEWS) program for wage percentile data by SOC code and geography.

No API key required. Rate limit: ~500 req/day from a single IP.
Endpoint docs: https://www.bls.gov/developers/api_signature_v2.htm

OEWS series ID format (25 chars):
  OEU + areatype(1) + area(7) + industry(6) + occupation(6) + datatype(2)
  areatype: N=national, S=state, M=MSA

Data type codes for OEWS:
  01 = Employment
  04 = Annual mean wage
  11 = Annual 10th pctile
  12 = Annual 25th pctile
  13 = Annual median
  14 = Annual 75th pctile
  15 = Annual 90th pctile
"""

import os
import requests
from typing import Optional


BLS_API_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_API_KEY  = os.getenv("BLS_API_KEY", "")


ANNUAL_DATATYPES = {
    "employment": "01",
    "mean":       "04",
    "pct10":      "11",
    "pct25":      "12",
    "median":     "13",
    "pct75":      "14",
    "pct90":      "15",
}


def _build_series_id(area_code: str, soc_code: str, datatype_code: str, area_type: str = "national") -> str:
    """
    Build a BLS OEWS series ID (25 chars).

    area_code  : BLS area code from geo.py
                 National → "0000000"
                 State    → "S4800000" (Texas)
                 MSA      → "M1242000" (Austin)
    soc_code   : e.g. "15-1252.00"
    area_type  : "national", "state", or "MSA"
    """
    occupation = soc_code.replace("-", "").replace(".", "")[:6]
    industry   = "000000"

    areatype_map = {"MSA": "M", "state": "S", "national": "N"}
    areatype = areatype_map.get(area_type, "N")

    # Strip letter prefix (M/S) to get the numeric portion
    numeric = area_code.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    if area_type == "MSA":
        # geo.py stores MSA codes as M{cbsa5}00 (trailing zeros).
        # BLS OEWS expects CBSA zero-padded to 7 digits with leading zeros.
        # e.g. "1242000"[:-2] = "12420" → f"{12420:07d}" = "0012420"
        cbsa = numeric[:-2]
        area = f"{int(cbsa):07d}"
    else:
        # State: "S4800000" → strip S → "4800000" (already correct 7 chars)
        # National: "0000000" (no prefix, already correct)
        area = numeric[:7].ljust(7, "0")

    return f"OEU{areatype}{area}{industry}{occupation}{datatype_code}"


class BLSClient:
    """Client for BLS OEWS public API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or BLS_API_KEY
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _fetch_series(self, series_ids: list[str]) -> dict:
        """Batch-fetch up to 25 series from BLS API v2."""
        payload = {
            "seriesid":  series_ids,
            "startyear": "2023",
            "endyear":   "2024",
            # NOTE: do NOT include "latest": True — it conflicts with year range
            # and causes REQUEST_FAILED_INVALID_PARAMETERS
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key

        try:
            resp = self.session.post(BLS_API_BASE, json=payload, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[BLS] Request error: {e}")
            return {}

    def get_oews(
        self,
        soc_code:  str,
        area_code: str = "0000000",
        area_type: str = "national",
    ) -> Optional[dict]:
        """
        Fetch OEWS wage percentile data for a SOC code + geography.

        Returns a dict with keys: pct10, pct25, median, pct75, pct90,
        mean, employment, year, area_code, area_type.
        Returns None if no data found or median is missing.
        """
        series_map = {}
        for name, code in ANNUAL_DATATYPES.items():
            sid = _build_series_id(area_code, soc_code, code, area_type)
            series_map[sid] = name

        raw = self._fetch_series(list(series_map.keys()))

        if not raw or raw.get("status") != "REQUEST_SUCCEEDED":
            print(f"[BLS] API status: {raw.get('status','unknown')} "
                  f"for area={area_code} soc={soc_code}")
            return None

        result = {"area_code": area_code, "area_type": area_type}

        for series in raw.get("Results", {}).get("series", []):
            sid  = series.get("seriesID", "")
            data = series.get("data", [])
            name = series_map.get(sid)
            if name and data:
                latest = data[0]
                val = latest.get("value")
                # BLS uses "-" for suppressed/unavailable data
                if val and val != "-":
                    result[name] = val
                if "year" not in result:
                    result["year"] = latest.get("year")

        if not result.get("median"):
            return None

        return result
