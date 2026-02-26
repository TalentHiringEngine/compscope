"""
Advanced JSearch Client — Live Job Posting Salary Intelligence
==============================================================
US-only. Queries at metro, state, and national levels separately
so results mirror the BLS geo cascade structure.

API: advanced-jsearch-job-search-salary-intelligence-api.p.rapidapi.com
"""

import os
import requests
from typing import Optional


JSEARCH_HOST = "advanced-jsearch-job-search-salary-intelligence-api.p.rapidapi.com"
JSEARCH_BASE = f"https://{JSEARCH_HOST}"


class JSearchClient:

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("JSEARCH_API_KEY", "")
        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key":  self.api_key,
            "x-rapidapi-host": JSEARCH_HOST,
        })

    def _fetch_salary(self, job_title: str, location: str) -> Optional[dict]:
        """
        Core salary insights fetch for a specific location string.
        Always restricts to US by appending country context.
        """
        # Append USA to location string to prevent international results
        us_location = location if "united states" in location.lower() else f"{location}, United States"

        params = {
            "job_title":        job_title,
            "location":         us_location,
            "radius":           "75",
            "employment_types": "FULLTIME",
            "country":          "us",
        }
        try:
            resp = self.session.get(
                f"{JSEARCH_BASE}/job-salary-insights",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[JSearch] Request error for '{location}': {e}")
            return None

        salary_data = data.get("data") or data.get("salary_data") or data
        if not salary_data:
            return None
        if isinstance(salary_data, list):
            if not salary_data:
                return None
            salary_data = salary_data[0]

        median  = (salary_data.get("median_salary") or
                   salary_data.get("median") or
                   salary_data.get("p50_salary"))
        pct25   = (salary_data.get("p25_salary") or salary_data.get("percentile_25"))
        pct75   = (salary_data.get("p75_salary") or salary_data.get("percentile_75"))
        min_sal = salary_data.get("min_salary") or salary_data.get("minimum_salary")
        max_sal = salary_data.get("max_salary") or salary_data.get("maximum_salary")
        count   = salary_data.get("job_count") or salary_data.get("count")

        rate = str(salary_data.get("salary_period", "yearly")).lower()

        def to_annual(v):
            try:
                v = float(v)
                return round(v * 2080) if rate in ("hourly", "hour", "hr") else round(v)
            except (ValueError, TypeError):
                return None

        median  = to_annual(median)
        pct25   = to_annual(pct25)
        pct75   = to_annual(pct75)
        min_sal = to_annual(min_sal)
        max_sal = to_annual(max_sal)

        if not median and not pct25 and not min_sal:
            return None

        return {
            "median":        median,
            "pct25":         pct25,
            "pct75":         pct75,
            "min":           min_sal,
            "max":           max_sal,
            "posting_count": count,
        }

    def get_geo_levels(
        self,
        job_title:  str,
        city:       str,
        state_name: str,
    ) -> dict:
        """
        Fetch salary data at metro, state, and national levels.
        Returns dict with keys 'metro', 'state', 'national' — each may be None.

        Args:
            job_title:  e.g. "Senior Data Engineer"
            city:       e.g. "Austin"
            state_name: e.g. "Texas"
        """
        results = {}

        # Metro level — city + state
        metro_loc = f"{city}, {state_name}"
        metro = self._fetch_salary(job_title, metro_loc)
        if metro:
            results["metro"] = {**metro, "geo_label": metro_loc}

        # State level — just the state
        state = self._fetch_salary(job_title, state_name)
        if state:
            results["state"] = {**state, "geo_label": state_name}

        # National level — United States
        national = self._fetch_salary(job_title, "United States")
        if national:
            results["national"] = {**national, "geo_label": "United States"}

        return results

    def search_jobs(self, job_title: str, location: str, max_results: int = 5) -> list[dict]:
        """Return individual US job postings with salary data for sample display."""
        us_location = f"{location}, United States"
        params = {
            "query":            f"{job_title} in {us_location}",
            "page":             "1",
            "num_pages":        "1",
            "employment_types": "FULLTIME",
            "country":          "us",
        }
        try:
            resp = self.session.get(f"{JSEARCH_BASE}/search", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[JSearch] Search error: {e}")
            return []

        jobs = []
        for job in (data.get("data") or [])[:max_results]:
            # Skip non-US postings
            job_country = str(job.get("job_country", "us")).lower()
            if job_country not in ("us", "usa", "united states", ""):
                continue

            min_sal = job.get("job_min_salary")
            max_sal = job.get("job_max_salary")
            period  = str(job.get("job_salary_period") or "").lower()

            def norm(v):
                try:
                    v = float(v)
                    return round(v * 2080) if period in ("hourly", "hour") else round(v)
                except (ValueError, TypeError):
                    return None

            min_sal = norm(min_sal)
            max_sal = norm(max_sal)
            if not min_sal:
                continue

            jobs.append({
                "title":      job.get("job_title", ""),
                "employer":   job.get("employer_name", ""),
                "location":   f"{job.get('job_city','')}, {job.get('job_state','')}",
                "salary_min": min_sal,
                "salary_max": max_sal,
                "url":        job.get("job_apply_link", ""),
                "posted":     (job.get("job_posted_at_datetime_utc") or "")[:10],
            })
        return jobs
