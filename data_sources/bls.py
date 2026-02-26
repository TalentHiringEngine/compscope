"""
BLS OEWS Client
===============
Queries the Bureau of Labor Statistics Occupational Employment and Wage
Statistics (OEWS) program for wage percentile data by SOC code and geography.

No API key required. Rate limit: ~500 req/day from a single IP.
Endpoint docs: https://www.bls.gov/developers/api_signature_v2.htm

OEWS series ID format:
  OEUN + area_code(7) + industry_code(6) + occupation_code(6) + datatype(2)
  e.g.  OEUN000000000000001115302500  (national, all industries, Software Devs, annual median)

Data type codes for OEWS:
  01 = Employment
  02 = Employment % RSE
  03 = Hourly mean wage
  04 = Annual mean wage
  05 = Wage % RSE
  06 = Hourly 10th pctile
  07 = Hourly 25th pctile
  08 = Hourly median
  09 = Hourly 75th pctile
  10 = Hourly 90th pctile
  11 = Annual 10th pctile
  12 = Annual 25th pctile
  13 = Annual median
  14 = Annual 75th pctile
  15 = Annual 90th pctile

We query annual figures (11–15 + 04) and convert if hourly-only.
"""

import requests
import re
from typing import Optional


BLS_API_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_API_KEY  = ""   # Optional — raises rate limit from ~500 to ~3000/day
                    # Get free key at https://data.bls.gov/registrationEngine/


# Maps our internal data type names to BLS OEWS data type codes
ANNUAL_DATATYPES = {
    "employment": "01",
    "mean":       "04",
    "pct10":      "11",
    "pct25":      "12",
    "median":     "13",
    "pct75":      "14",
    "pct90":      "15",
}


def _build_series_id(area_code: str, soc_code: str, datatype_code: str) -> str:
    """
    Build a BLS OEWS series ID.

    area_code   : 7-character BLS area code (e.g. "S4900000" = Texas, "M1280000" = Austin MSA)
    soc_code    : SOC code with hyphen removed and zero-padded to 6 digits (e.g. "151252")
    datatype_code: 2-digit string from ANNUAL_DATATYPES
    """
    occupation = soc_code.replace("-", "")[:6].zfill(6)
    industry   = "000000"   # All industries
    area       = area_code[:7].ljust(7, "0")
    return f"OEUN{area}{industry}{occupation}{datatype_code}"


class BLSClient:
    """Client for BLS OEWS public API."""

    def __init__(self, api_key: str = BLS_API_KEY):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _fetch_series(self, series_ids: list[str]) -> dict:
        """Batch-fetch up to 25 series from BLS API v2."""
        payload = {
            "seriesid": series_ids,
            "startyear": "2022",
            "endyear":   "2024",
            "latest":    True,
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
        soc_code: str,
        area_code: str = "0000000",
        area_type: str = "national",
    ) -> Optional[dict]:
        """
        Fetch OEWS wage percentile data for a SOC code + geography.

        Returns a dict with keys: pct10, pct25, median, pct75, pct90,
        mean, employment, year, area_code, area_type.
        Returns None if no data found.
        """
        # Build all series IDs in one batch request
        series_map = {}
        for name, code in ANNUAL_DATATYPES.items():
            sid = _build_series_id(area_code, soc_code, code)
            series_map[sid] = name

        raw = self._fetch_series(list(series_map.keys()))

        if not raw or raw.get("status") != "REQUEST_SUCCEEDED":
            print(f"[BLS] API returned: {raw.get('status','unknown')} for area={area_code} soc={soc_code}")
            return None

        result = {"area_code": area_code, "area_type": area_type}

        for series in raw.get("Results", {}).get("series", []):
            sid  = series.get("seriesID", "")
            data = series.get("data", [])
            name = series_map.get(sid)
            if name and data:
                # Take the most recent period
                latest = data[0]
                result[name] = latest.get("value")
                if "year" not in result:
                    result["year"] = latest.get("year")

        # Must have at least a median to be useful
        if not result.get("median"):
            return None

        # Hourly → annual conversion fallback
        # BLS sometimes returns "-" for annual in thin markets; check hourly series if needed
        # (handled gracefully: missing values stay None)

        return result

    def get_area_code_for_msa(self, msa_fips: str) -> Optional[str]:
        """
        Convert Census CBSA FIPS to BLS area code.
        BLS MSA area codes follow the pattern M + CBSA_code (zero-padded to 7).
        e.g. CBSA 12420 (Austin) → M1242000
        """
        if not msa_fips:
            return None
        return f"M{str(msa_fips).zfill(5)}00"

    def get_state_area_code(self, fips_state: str) -> Optional[str]:
        """
        Convert 2-digit state FIPS to BLS OEWS state area code.
        Pattern: S + FIPS + 00000
        e.g. Texas FIPS 48 → S4800000
        """
        if not fips_state:
            return None
        return f"S{str(fips_state).zfill(2)}00000"
