"""
JSearch Client — Live Job Posting Salary Intelligence
=====================================================
Uses the JSearch /search endpoint (jsearch.p.rapidapi.com).
Fetches job postings, filters those with salary data, and
aggregates into median/percentile estimates at metro, state,
and national levels.

Most postings don't include salary — we fetch 3 pages (~30 jobs)
and compute statistics from whichever ones do disclose pay.

Sign up: https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch
Set JSEARCH_API_KEY in Streamlit secrets.
"""

import os
import statistics
import requests
from typing import Optional


JSEARCH_HOST = "jsearch.p.rapidapi.com"
JSEARCH_BASE = f"https://{JSEARCH_HOST}"


def _to_annual(value, period: str) -> Optional[float]:
    """Convert a salary value to annual based on its period."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    period = str(period or "").upper()
    if period in ("HOUR", "HOURLY"):
        return round(v * 2080)
    if period in ("MONTH", "MONTHLY"):
        return round(v * 12)
    return round(v)  # YEAR or unknown — treat as annual


class JSearchClient:

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("JSEARCH_API_KEY", "")
        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key":  self.api_key,
            "x-rapidapi-host": JSEARCH_HOST,
        })

    def _fetch_jobs(self, query: str, num_pages: int = 7) -> list[dict]:
        """
        Fetch job postings from JSearch /search endpoint.
        Returns only full-time, US-based postings.
        """
        if not self.api_key:
            return []

        params = {
            "query":       query,
            "num_pages":   str(num_pages),
            "page":        "1",
            "country":     "us",
            "language":    "en",
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
            print(f"[JSearch] Request error for '{query}': {e}")
            return []

        jobs = []
        for job in data.get("data", []):
            # US only
            if str(job.get("job_country", "US")).upper() not in ("US", "USA", "UNITED STATES", ""):
                continue
            # Full-time preferred but include all for salary purposes
            jobs.append(job)

        return jobs

    def _salary_stats(self, jobs: list[dict]) -> Optional[dict]:
        """
        Extract salary data from postings and compute statistics.
        Returns None if fewer than 2 postings have salary data.
        """
        midpoints = []
        mins, maxs = [], []

        for job in jobs:
            lo = _to_annual(job.get("job_min_salary"), job.get("job_salary_period"))
            hi = _to_annual(job.get("job_max_salary"), job.get("job_salary_period"))

            # Skip implausible values (< $15k or > $1M)
            if lo and lo < 15000: lo = None
            if hi and hi > 1000000: hi = None

            if lo and hi:
                midpoints.append((lo + hi) / 2)
                mins.append(lo)
                maxs.append(hi)
            elif lo:
                midpoints.append(lo)
                mins.append(lo)
            elif hi:
                midpoints.append(hi)
                maxs.append(hi)

        if len(midpoints) < 2:
            return None

        midpoints.sort()
        return {
            "median":        round(statistics.median(midpoints)),
            "pct25":         round(midpoints[len(midpoints) // 4]),
            "pct75":         round(midpoints[3 * len(midpoints) // 4]),
            "min":           round(min(mins)) if mins else None,
            "max":           round(max(maxs)) if maxs else None,
            "posting_count": len(midpoints),
        }

    def get_geo_levels(self, job_title: str, city: str, state_name: str) -> dict:
        """
        Fetch salary data at metro, state, and national levels
        by querying JSearch with location-scoped queries.

        Uses the user's original job title — not the BLS SOC title.
        """
        if not self.api_key:
            print("[JSearch] No API key configured.")
            return {}

        results = {}

        # Metro level
        metro_jobs = self._fetch_jobs(f"{job_title} jobs in {city}, {state_name}")
        metro_stats = self._salary_stats(metro_jobs)
        if metro_stats:
            results["metro"] = {**metro_stats, "geo_label": f"{city}, {state_name}"}

        # State level
        state_jobs = self._fetch_jobs(f"{job_title} jobs in {state_name}")
        state_stats = self._salary_stats(state_jobs)
        if state_stats:
            results["state"] = {**state_stats, "geo_label": state_name}

        # National level
        natl_jobs = self._fetch_jobs(f"{job_title} jobs in United States")
        natl_stats = self._salary_stats(natl_jobs)
        if natl_stats:
            results["national"] = {**natl_stats, "geo_label": "United States"}

        return results

    def get_sample_postings(self, job_title: str, location: str, max_results: int = 5) -> list[dict]:
        """
        Return individual job postings with salary data for display.
        Uses the user's original job title.
        """
        jobs = self._fetch_jobs(f"{job_title} jobs in {location}")
        out = []
        for job in jobs:
            lo = _to_annual(job.get("job_min_salary"), job.get("job_salary_period"))
            hi = _to_annual(job.get("job_max_salary"), job.get("job_salary_period"))
            if not lo:
                continue
            out.append({
                "title":      job.get("job_title", ""),
                "employer":   job.get("employer_name", ""),
                "location":   f"{job.get('job_city','')}, {job.get('job_state','')}",
                "salary_min": lo,
                "salary_max": hi,
                "url":        job.get("job_apply_link", ""),
                "posted":     (job.get("job_posted_at_datetime_utc") or "")[:10],
            })
            if len(out) >= max_results:
                break
        return out
