"""
Adzuna Job Postings Client
==========================
Queries Adzuna's free job search API for postings with salary data.
Adzuna aggregates from Indeed, CareerBuilder, and other job boards.

Free API: 50,000 requests/month
Register at: https://developer.adzuna.com/signup
Docs: https://developer.adzuna.com/overview

Set env vars:
  ADZUNA_APP_ID=your_app_id
  ADZUNA_APP_KEY=your_app_key
"""

import os
import requests
from typing import Optional


ADZUNA_BASE = "https://api.adzuna.com/v1/api/jobs/us/search/1"


def _percentile(data: list, pct: float) -> float:
    idx   = (pct / 100) * (len(data) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(data) - 1)
    return data[lower] + (idx - lower) * (data[upper] - data[lower])


class AdzunaClient:

    def __init__(self, app_id: str = None, app_key: str = None):
        self.app_id  = app_id  or os.getenv("ADZUNA_APP_ID", "")
        self.app_key = app_key or os.getenv("ADZUNA_APP_KEY", "")
        self.session = requests.Session()

    def search(self, job_title: str, location: str, max_results: int = 50) -> Optional[dict]:
        """
        Search postings with salary data and return aggregated stats.

        Returns:
            {
              "count": int,
              "pct25": float, "median": float, "pct75": float,
              "min": float, "max": float,
              "postings": list[dict],   # top 10 individual postings
              "source": "adzuna",
            }
        or None if no credentials / no results.
        """
        if not self.app_id or not self.app_key:
            return None

        params = {
            "app_id":                 self.app_id,
            "app_key":                self.app_key,
            "what":                   job_title,
            "where":                  location,
            "results_per_page":       min(max_results, 50),
            "salary_include_unknown": 0,
        }

        try:
            resp = self.session.get(ADZUNA_BASE, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[Adzuna] Request error: {e}")
            return None

        postings = []
        salaries = []

        for job in data.get("results", []):
            s_min = job.get("salary_min")
            s_max = job.get("salary_max")
            if not s_min:
                continue

            mid = (s_min + s_max) / 2 if s_max else s_min
            salaries.append(mid)

            postings.append({
                "title":      job.get("title", ""),
                "company":    job.get("company", {}).get("display_name", ""),
                "salary_min": s_min,
                "salary_max": s_max,
                "location":   job.get("location", {}).get("display_name", ""),
                "url":        job.get("redirect_url", ""),
                "created":    job.get("created", "")[:10] if job.get("created") else "",
            })

        if not salaries:
            return None

        s = sorted(salaries)
        return {
            "count":    len(s),
            "pct25":    round(_percentile(s, 25)),
            "median":   round(_percentile(s, 50)),
            "pct75":    round(_percentile(s, 75)),
            "min":      round(min(s)),
            "max":      round(max(s)),
            "postings": postings[:10],
            "source":   "adzuna",
        }
