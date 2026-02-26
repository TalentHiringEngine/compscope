"""
USAJobs Client
==============
Queries the USAJOBS.gov public API for federal job postings
with salary range data. Useful as a real-time salary transparency
data source (all federal postings include pay band info).

API docs: https://developer.usajobs.gov/
Requires free registration for an API key:
  https://developer.usajobs.gov/apirequest/

Set env vars: USAJOBS_API_KEY, USAJOBS_USER_AGENT (your email)
"""

import os
import requests
from typing import Optional


USAJOBS_BASE = "https://data.usajobs.gov/api/search"


class USAJobsClient:

    def __init__(
        self,
        api_key:    str = None,
        user_agent: str = None,
    ):
        self.api_key    = api_key    or os.getenv("USAJOBS_API_KEY", "")
        self.user_agent = user_agent or os.getenv("USAJOBS_USER_AGENT", "compscope@example.com")
        self.session    = requests.Session()
        self.session.headers.update({
            "Authorization-Key": self.api_key,
            "User-Agent":        self.user_agent,
            "Host":              "data.usajobs.gov",
        })

    def search(self, keyword: str, location: str, max_results: int = 20) -> list[dict]:
        """
        Search USAJobs for postings matching keyword near location.
        Returns a list of dicts with title, salary_min, salary_max,
        pay_scale, location, url.
        """
        if not self.api_key:
            # No key — return empty with a note
            print("[USAJobs] No API key configured. Set USAJOBS_API_KEY env var.")
            return []

        # Parse "City, State" → just the state or full string
        params = {
            "Keyword":         keyword,
            "LocationName":    location,
            "ResultsPerPage":  max_results,
            "SalaryBucket":    "all",
            "Fields":          "Min",
        }

        try:
            resp = self.session.get(USAJOBS_BASE, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[USAJobs] Request error: {e}")
            return []

        jobs = []
        for item in data.get("SearchResult", {}).get("SearchResultItems", []):
            mv = item.get("MatchedObjectDescriptor", {})
            remuneration = mv.get("PositionRemuneration", [{}])[0]

            salary_min = remuneration.get("MinimumRange")
            salary_max = remuneration.get("MaximumRange")
            pay_rate   = remuneration.get("RateIntervalCode", "")  # PA = per annum

            # Normalize to annual
            try:
                s_min = float(salary_min) if salary_min else None
                s_max = float(salary_max) if salary_max else None
                if pay_rate == "PH" and s_min:   # hourly → annual
                    s_min = s_min * 2080
                    s_max = s_max * 2080 if s_max else None
            except (ValueError, TypeError):
                s_min, s_max = None, None

            if not s_min:
                continue  # Skip postings without salary data

            locations = mv.get("PositionLocation", [{}])
            loc_str = locations[0].get("LocationName", "") if locations else ""

            jobs.append({
                "title":      mv.get("PositionTitle", ""),
                "salary_min": s_min,
                "salary_max": s_max,
                "pay_scale":  mv.get("JobGrade", [{}])[0].get("Code", "") + " " + pay_rate,
                "location":   loc_str,
                "url":        mv.get("ApplyURI", [""])[0],
            })

        return jobs
