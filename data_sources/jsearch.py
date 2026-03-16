"""
JSearch Mega Client — Live Salary Intelligence
===============================================
Uses jsearch-mega.p.rapidapi.com endpoints:
  - /estimated-salary  → direct salary estimates by title + location
  - /search            → individual job postings for company hiring list

Sign up: https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch
Set JSEARCH_API_KEY in Streamlit secrets or env vars.
"""

import os
import requests
from typing import Optional


JSEARCH_HOST = "jsearch-mega.p.rapidapi.com"
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

    def _estimated_salary(self, job_title: str, location: str, location_type: str = "ANY", years_of_experience: str = "ALL") -> Optional[dict]:
        """
        Call /estimated-salary endpoint for a title + location.
        Returns aggregated salary stats or None.
        """
        if not self.api_key:
            return None

        params = {
            "job_title":           job_title,
            "location":            location,
            "location_type":       location_type,
            "years_of_experience": years_of_experience or "ALL",
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

        e = entries[0]
        period = e.get("salary_period", "YEAR")

        median = _to_annual(e.get("median_salary"), period)
        lo     = _to_annual(e.get("min_salary"),    period)
        hi     = _to_annual(e.get("max_salary"),    period)
        med_base       = _to_annual(e.get("median_base_salary"),       period)
        med_additional = _to_annual(e.get("median_additional_pay"),    period)

        if not median:
            return None

        return {
            "median":           median,
            "min":              lo,
            "max":              hi,
            "pct25":            lo,
            "pct75":            hi,
            "median_base":      med_base,
            "median_additional": med_additional,
            "posting_count":    e.get("salary_count", 0),
            "publisher":        e.get("publisher_name", ""),
            "confidence":       e.get("confidence", ""),
        }

    def get_geo_levels(self, job_title: str, city: str, state_name: str, years_of_experience: str = "ALL") -> dict:
        """
        Fetch salary estimates at metro, state, and national levels.
        Falls back to broader geo if no data found. Always returns something if API is reachable.
        """
        if not self.api_key:
            return {}

        yoe = years_of_experience or "ALL"
        results = {}

        metro = self._estimated_salary(job_title, f"{city}, {state_name}", "CITY", yoe)
        if metro:
            results["metro"] = {**metro, "geo_label": f"{city}, {state_name}"}

        if "metro" not in results:
            state = self._estimated_salary(job_title, state_name, "STATE", yoe)
            if state:
                results["state"] = {**state, "geo_label": state_name}

        if not results:
            natl = self._estimated_salary(job_title, "United States", "COUNTRY", yoe)
            if natl:
                results["national"] = {**natl, "geo_label": "United States"}

        # If still nothing and a specific exp level was chosen, retry with ALL
        if not results and yoe != "ALL":
            metro = self._estimated_salary(job_title, f"{city}, {state_name}", "CITY", "ALL")
            if metro:
                results["metro"] = {**metro, "geo_label": f"{city}, {state_name}"}
            if not results:
                natl = self._estimated_salary(job_title, "United States", "COUNTRY", "ALL")
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
