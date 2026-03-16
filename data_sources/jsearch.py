"""
JSearch Client — Live Job Posting Salary Intelligence
=====================================================
Uses jsearch27.p.rapidapi.com endpoints:
  - /estimated-salary  → direct salary estimates by title + location
  - /search            → individual job postings for company hiring list

Sign up: https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch
Set JSEARCH_API_KEY in Streamlit secrets or env vars.
"""

import os
import requests
from typing import Optional


JSEARCH_HOST = "jsearch27.p.rapidapi.com"
JSEARCH_BASE = f"https://{JSEARCH_HOST}"


def _to_annual(value, period: str) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    period = str(period or "").upper()
    if period in ("HOUR", "HOURLY"):
        return round(v * 2080)
    if period in ("MONTH", "MONTHLY"):
        return round(v * 12)
    return round(v)


class JSearchClient:

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("JSEARCH_API_KEY", "")
        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key":  self.api_key,
            "x-rapidapi-host": JSEARCH_HOST,
        })

    def _estimated_salary(self, job_title: str, location: str, radius: int = 100) -> Optional[dict]:
        """
        Call /estimated-salary endpoint for a title + location.
        Returns aggregated salary stats or None.
        """
        if not self.api_key:
            return None
        params = {
            "job_title": job_title,
            "location":  location,
            "radius":    str(radius),
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
            print(f"[JSearch] estimated-salary error for '{job_title}' @ '{location}': {e}")
            return None

        entries = data.get("data", [])
        if not entries:
            return None

        # Collect all salary points across returned estimates
        mins, maxs, meds = [], [], []
        for e in entries:
            period = e.get("salary_period", "YEAR")
            lo  = _to_annual(e.get("min_salary"),    period)
            hi  = _to_annual(e.get("max_salary"),    period)
            med = _to_annual(e.get("median_salary"), period)
            if lo  and 15000 < lo  < 1_000_000: mins.append(lo)
            if hi  and 15000 < hi  < 1_000_000: maxs.append(hi)
            if med and 15000 < med < 1_000_000: meds.append(med)

        all_points = meds or (mins + maxs)
        if not all_points:
            return None

        all_points.sort()
        n = len(all_points)
        return {
            "median":        round(all_points[n // 2]),
            "pct25":         round(all_points[n // 4]),
            "pct75":         round(all_points[3 * n // 4]),
            "min":           round(min(mins)) if mins else None,
            "max":           round(max(maxs)) if maxs else None,
            "posting_count": n,
        }

    def get_geo_levels(self, job_title: str, city: str, state_name: str) -> dict:
        """
        Fetch salary estimates at metro, state, and national levels.
        """
        if not self.api_key:
            return {}

        results = {}

        metro = self._estimated_salary(job_title, f"{city}, {state_name}")
        if metro:
            results["metro"] = {**metro, "geo_label": f"{city}, {state_name}"}

        if "metro" not in results:
            state = self._estimated_salary(job_title, state_name)
            if state:
                results["state"] = {**state, "geo_label": state_name}

        if not results:
            natl = self._estimated_salary(job_title, "United States")
            if natl:
                results["national"] = {**natl, "geo_label": "United States"}

        return results

    def get_sample_postings(self, job_title: str, location: str, max_results: int = 10) -> list[dict]:
        """
        Return individual job postings from /search for the hiring companies list.
        """
        if not self.api_key:
            return []

        params = {
            "query":       f"{job_title} in {location}",
            "num_pages":   "3",
            "page":        "1",
            "country":     "us",
            "date_posted": "all",
        }
        try:
            resp = self.session.get(
                f"{JSEARCH_BASE}/search",
                params=params,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[JSearch] search error for '{job_title}': {e}")
            return []

        out = []
        for job in data.get("data", []):
            lo = _to_annual(job.get("job_min_salary"), job.get("job_salary_period"))
            hi = _to_annual(job.get("job_max_salary"), job.get("job_salary_period"))
            out.append({
                "title":      job.get("job_title", ""),
                "employer":   job.get("employer_name", ""),
                "location":   f"{job.get('job_city', '')}, {job.get('job_state', '')}".strip(", "),
                "salary_min": lo,
                "salary_max": hi,
                "url":        job.get("job_apply_link", ""),
                "posted":     (job.get("job_posted_at_datetime_utc") or "")[:10],
            })
            if len(out) >= max_results:
                break
        return out
