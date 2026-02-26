"""
JSearch Client — Live Job Posting Salary Intelligence
=====================================================
Uses the standard JSearch API on RapidAPI (jsearch.p.rapidapi.com).
Queries the /estimated-salary endpoint at metro, state, and national
levels so results mirror the BLS geo structure.

Sign up: https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch
Set JSEARCH_API_KEY in Streamlit secrets.
"""

import os
import requests
from typing import Optional


JSEARCH_HOST = "jsearch.p.rapidapi.com"
JSEARCH_BASE = f"https://{JSEARCH_HOST}"


class JSearchClient:

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("JSEARCH_API_KEY", "")
        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key":  self.api_key,
            "x-rapidapi-host": JSEARCH_HOST,
        })

    def _fetch_salary(self, job_title: str, location: str, location_type: str) -> Optional[dict]:
        """
        Fetch estimated salary data for a job title + location.

        location_type: "CITY" | "STATE" | "COUNTRY"
        """
        if not self.api_key:
            return None

        params = {
            "job_title":          job_title,
            "location":           location,
            "location_type":      location_type,
            "years_of_experience": "ALL",
        }
        try:
            resp = self.session.get(
                f"{JSEARCH_BASE}/estimated-salary",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[JSearch] Request error for '{location}' ({location_type}): {e}")
            return None

        rows = data.get("data", [])
        if not rows:
            return None

        # Average across all returned salary estimates for this location
        medians, lows, highs = [], [], []
        for row in rows:
            if str(row.get("salary_currency", "USD")).upper() != "USD":
                continue
            period = str(row.get("salary_period", "YEAR")).upper()
            def to_annual(v):
                try:
                    v = float(v)
                    return round(v * 2080) if period in ("HOUR", "HOURLY") else round(v)
                except (ValueError, TypeError):
                    return None

            med = to_annual(row.get("median_salary"))
            lo  = to_annual(row.get("min_salary"))
            hi  = to_annual(row.get("max_salary"))
            if med and med > 10000:
                medians.append(med)
            if lo:  lows.append(lo)
            if hi:  highs.append(hi)

        if not medians:
            return None

        return {
            "median":        round(sum(medians) / len(medians)),
            "pct25":         round(min(lows))    if lows   else None,
            "pct75":         round(max(highs))   if highs  else None,
            "posting_count": len(rows),
        }

    def get_geo_levels(self, job_title: str, city: str, state_name: str) -> dict:
        """
        Fetch salary data at metro, state, and national levels.
        Returns dict with keys 'metro', 'state', 'national'.
        """
        if not self.api_key:
            print("[JSearch] No API key configured.")
            return {}

        results = {}

        metro = self._fetch_salary(job_title, f"{city}, {state_name}", "CITY")
        if metro:
            results["metro"] = {**metro, "geo_label": f"{city}, {state_name}"}

        state = self._fetch_salary(job_title, state_name, "STATE")
        if state:
            results["state"] = {**state, "geo_label": state_name}

        national = self._fetch_salary(job_title, "United States", "COUNTRY")
        if national:
            results["national"] = {**national, "geo_label": "United States"}

        return results
