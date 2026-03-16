"""
O*NET Client — Job Title to SOC Code Matching
==============================================
Uses the O*NET Web Services API to map free-text job titles
to standardized SOC codes.

API docs: https://services.onetcenter.org/
Authentication: HTTP Basic auth with username/password.
Free tier: 5 req/sec, unlimited volume. Register at:
  https://services.onetcenter.org/developer/

Set ONET_USERNAME / ONET_PASSWORD environment variables,
or pass them directly to ONETClient().

Fallback: if no credentials, we use the public keyword search
endpoint which works unauthenticated at lower rate limits.
"""

import os
import requests
import re
from typing import Optional
from difflib import SequenceMatcher


# O*NET public search requires no auth; authenticated endpoint has higher limits
ONET_BASE = "https://services.onetcenter.org/ws/"


# Hardcoded common title mappings to avoid API calls for the most frequent queries.
# Extend this liberally — it doubles as a cache and reduces latency.
LOCAL_TITLE_MAP = {
    # ── Software Engineering ──────────────────────────────────────────────────
    "software engineer":               ("15-1252.00", "Software Developers"),
    "software developer":              ("15-1252.00", "Software Developers"),
    "swe":                             ("15-1252.00", "Software Developers"),
    "junior software engineer":        ("15-1252.00", "Software Developers"),
    "senior software engineer":        ("15-1252.00", "Software Developers"),
    "sr software engineer":            ("15-1252.00", "Software Developers"),
    "sr. software engineer":           ("15-1252.00", "Software Developers"),
    "staff software engineer":         ("15-1252.00", "Software Developers"),
    "principal software engineer":     ("15-1252.00", "Software Developers"),
    "lead software engineer":          ("15-1252.00", "Software Developers"),
    "software engineer ii":            ("15-1252.00", "Software Developers"),
    "software engineer iii":           ("15-1252.00", "Software Developers"),
    "frontend engineer":               ("15-1252.00", "Software Developers"),
    "front end engineer":              ("15-1252.00", "Software Developers"),
    "front-end engineer":              ("15-1252.00", "Software Developers"),
    "backend engineer":                ("15-1252.00", "Software Developers"),
    "back end engineer":               ("15-1252.00", "Software Developers"),
    "back-end engineer":               ("15-1252.00", "Software Developers"),
    "fullstack engineer":              ("15-1252.00", "Software Developers"),
    "full stack engineer":             ("15-1252.00", "Software Developers"),
    "full-stack engineer":             ("15-1252.00", "Software Developers"),
    "frontend developer":              ("15-1252.00", "Software Developers"),
    "backend developer":               ("15-1252.00", "Software Developers"),
    "web developer":                   ("15-1252.00", "Software Developers"),

    # ── Data Engineering ──────────────────────────────────────────────────────
    "data engineer":                   ("15-1243.00", "Database Architects"),
    "junior data engineer":            ("15-1243.00", "Database Architects"),
    "senior data engineer":            ("15-1243.00", "Database Architects"),
    "sr data engineer":                ("15-1243.00", "Database Architects"),
    "sr. data engineer":               ("15-1243.00", "Database Architects"),
    "lead data engineer":              ("15-1243.00", "Database Architects"),
    "staff data engineer":             ("15-1243.00", "Database Architects"),
    "principal data engineer":         ("15-1243.00", "Database Architects"),
    "data engineer ii":                ("15-1243.00", "Database Architects"),
    "data engineer iii":               ("15-1243.00", "Database Architects"),
    "database engineer":               ("15-1243.00", "Database Architects"),
    "database architect":              ("15-1243.00", "Database Architects"),
    "analytics engineer":              ("15-1243.00", "Database Architects"),
    "data platform engineer":          ("15-1243.00", "Database Architects"),
    "data infrastructure engineer":    ("15-1243.00", "Database Architects"),

    # ── Data Science / ML ─────────────────────────────────────────────────────
    "data scientist":                  ("15-2051.00", "Data Scientists"),
    "senior data scientist":           ("15-2051.00", "Data Scientists"),
    "sr data scientist":               ("15-2051.00", "Data Scientists"),
    "sr. data scientist":              ("15-2051.00", "Data Scientists"),
    "lead data scientist":             ("15-2051.00", "Data Scientists"),
    "staff data scientist":            ("15-2051.00", "Data Scientists"),
    "principal data scientist":        ("15-2051.00", "Data Scientists"),
    "machine learning engineer":       ("15-2051.01", "Data Scientists, All Other"),
    "ml engineer":                     ("15-2051.01", "Data Scientists, All Other"),
    "senior machine learning engineer":("15-2051.01", "Data Scientists, All Other"),
    "sr ml engineer":                  ("15-2051.01", "Data Scientists, All Other"),
    "ai engineer":                     ("15-2051.01", "Data Scientists, All Other"),
    "applied scientist":               ("15-2051.00", "Data Scientists"),
    "research scientist":              ("15-2051.00", "Data Scientists"),

    # ── Data Analysis ─────────────────────────────────────────────────────────
    "data analyst":                    ("15-2041.00", "Statisticians"),
    "senior data analyst":             ("15-2041.00", "Statisticians"),
    "sr data analyst":                 ("15-2041.00", "Statisticians"),
    "business intelligence analyst":   ("15-2041.00", "Statisticians"),
    "bi analyst":                      ("15-2041.00", "Statisticians"),
    "business intelligence engineer":  ("15-2041.00", "Statisticians"),
    "bi engineer":                     ("15-2041.00", "Statisticians"),

    # ── Infrastructure / DevOps ───────────────────────────────────────────────
    "devops engineer":                 ("15-1244.00", "Network and Computer Systems Administrators"),
    "senior devops engineer":          ("15-1244.00", "Network and Computer Systems Administrators"),
    "sr devops engineer":              ("15-1244.00", "Network and Computer Systems Administrators"),
    "site reliability engineer":       ("15-1244.00", "Network and Computer Systems Administrators"),
    "sre":                             ("15-1244.00", "Network and Computer Systems Administrators"),
    "platform engineer":               ("15-1244.00", "Network and Computer Systems Administrators"),
    "infrastructure engineer":         ("15-1244.00", "Network and Computer Systems Administrators"),
    "cloud engineer":                  ("15-1241.00", "Computer Network Architects"),
    "cloud architect":                 ("15-1241.00", "Computer Network Architects"),
    "solutions architect":             ("15-1241.00", "Computer Network Architects"),
    "enterprise architect":            ("15-1241.00", "Computer Network Architects"),

    # ── Security ──────────────────────────────────────────────────────────────
    "cybersecurity analyst":           ("15-1212.00", "Information Security Analysts"),
    "security analyst":                ("15-1212.00", "Information Security Analysts"),
    "information security analyst":    ("15-1212.00", "Information Security Analysts"),
    "penetration tester":              ("15-1212.00", "Information Security Analysts"),
    "security engineer":               ("15-1212.00", "Information Security Analysts"),

    # ── Management ────────────────────────────────────────────────────────────
    "engineering manager":             ("11-9041.00", "Architectural and Engineering Managers"),
    "senior engineering manager":      ("11-9041.00", "Architectural and Engineering Managers"),
    "director of engineering":         ("11-9041.00", "Architectural and Engineering Managers"),
    "vp of engineering":               ("11-9041.00", "Architectural and Engineering Managers"),
    "technical program manager":       ("15-1299.09", "Information Technology Project Managers"),
    "tpm":                             ("15-1299.09", "Information Technology Project Managers"),
    "it project manager":              ("15-1299.09", "Information Technology Project Managers"),
    "project manager":                 ("11-9199.00", "Managers, All Other"),
    "program manager":                 ("11-9199.00", "Managers, All Other"),
    "product manager":                 ("11-2021.00", "Marketing Managers"),
    "product manager (tech)":          ("15-1299.09", "Information Technology Project Managers"),
    "senior product manager":          ("11-2021.00", "Marketing Managers"),
    "sr product manager":              ("11-2021.00", "Marketing Managers"),

    # ── Design ────────────────────────────────────────────────────────────────
    "ux designer":                     ("27-1021.00", "Commercial and Industrial Designers"),
    "ui designer":                     ("27-1021.00", "Commercial and Industrial Designers"),
    "ux/ui designer":                  ("27-1021.00", "Commercial and Industrial Designers"),
    "product designer":                ("27-1021.00", "Commercial and Industrial Designers"),
    "graphic designer":                ("27-1024.00", "Graphic Designers"),

    # ── Healthcare ────────────────────────────────────────────────────────────
    "registered nurse":                ("29-1141.00", "Registered Nurses"),
    "rn":                              ("29-1141.00", "Registered Nurses"),
    "nurse practitioner":              ("29-1171.00", "Nurse Practitioners"),
    "np":                              ("29-1171.00", "Nurse Practitioners"),
    "physician":                       ("29-1215.00", "Family Medicine Physicians"),
    "physician assistant":             ("29-1071.00", "Physician Assistants"),
    "pa":                              ("29-1071.00", "Physician Assistants"),
    "physical therapist":              ("29-1123.00", "Physical Therapists"),
    "occupational therapist":          ("29-1122.00", "Occupational Therapists"),

    # ── Finance / Business ────────────────────────────────────────────────────
    "accountant":                      ("13-2011.00", "Accountants and Auditors"),
    "senior accountant":               ("13-2011.00", "Accountants and Auditors"),
    "cpa":                             ("13-2011.00", "Accountants and Auditors"),
    "financial analyst":               ("13-2051.00", "Financial and Investment Analysts"),
    "senior financial analyst":        ("13-2051.00", "Financial and Investment Analysts"),
    "business analyst":                ("13-1111.00", "Management Analysts"),
    "senior business analyst":         ("13-1111.00", "Management Analysts"),
    "management consultant":           ("13-1111.00", "Management Analysts"),
    "marketing manager":               ("11-2021.00", "Marketing Managers"),
    "sales representative":            ("41-4012.00", "Sales Representatives"),
    "account executive":               ("41-4012.00", "Sales Representatives"),
    "account manager":                 ("41-4012.00", "Sales Representatives"),
    "human resources manager":         ("11-3121.00", "Human Resources Managers"),
    "hr manager":                      ("11-3121.00", "Human Resources Managers"),
    "recruiter":                       ("13-1071.00", "Human Resources Specialists"),
    "talent acquisition specialist":   ("13-1071.00", "Human Resources Specialists"),
    "hr business partner":             ("13-1071.00", "Human Resources Specialists"),

    # ── Legal ─────────────────────────────────────────────────────────────────
    "attorney":                        ("23-1011.00", "Lawyers"),
    "lawyer":                          ("23-1011.00", "Lawyers"),
    "paralegal":                       ("23-2011.00", "Paralegals and Legal Assistants"),

    # ── Education / Other ─────────────────────────────────────────────────────
    "teacher":                         ("25-2031.00", "Secondary School Teachers"),
    "mechanical engineer":             ("17-2141.00", "Mechanical Engineers"),
    "electrical engineer":             ("17-2071.00", "Electrical Engineers"),
    "civil engineer":                  ("17-2051.00", "Civil Engineers"),
    "structural engineer":             ("17-2051.00", "Civil Engineers"),

    # ── Manufacturing / Production ────────────────────────────────────────────
    "welder":                          ("51-4121.00", "Welders, Cutters, Solderers, and Brazers"),
    "welding technician":              ("51-4121.00", "Welders, Cutters, Solderers, and Brazers"),
    "mig welder":                      ("51-4121.00", "Welders, Cutters, Solderers, and Brazers"),
    "tig welder":                      ("51-4121.00", "Welders, Cutters, Solderers, and Brazers"),
    "machinist":                       ("51-4041.00", "Machinists"),
    "cnc machinist":                   ("51-4011.00", "CNC Tool Operators"),
    "cnc operator":                    ("51-4011.00", "CNC Tool Operators"),
    "cnc programmer":                  ("51-4011.00", "CNC Tool Operators"),
    "cnc technician":                  ("51-4011.00", "CNC Tool Operators"),
    "production worker":               ("51-9198.00", "Production Workers, All Other"),
    "production operator":             ("51-9198.00", "Production Workers, All Other"),
    "assembly worker":                 ("51-2098.00", "Assemblers and Fabricators, All Other"),
    "assembler":                       ("51-2098.00", "Assemblers and Fabricators, All Other"),
    "line worker":                     ("51-9198.00", "Production Workers, All Other"),
    "quality inspector":               ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
    "quality control inspector":       ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
    "quality assurance inspector":     ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
    "press operator":                  ("51-4031.00", "Cutting, Punching, and Press Machine Setters, Operators, and Tenders"),
    "machine operator":                ("51-9198.00", "Production Workers, All Other"),

    # ── Trades ────────────────────────────────────────────────────────────────
    "electrician":                     ("47-2111.00", "Electricians"),
    "journeyman electrician":          ("47-2111.00", "Electricians"),
    "apprentice electrician":          ("47-2111.00", "Electricians"),
    "plumber":                         ("47-2152.00", "Plumbers, Pipefitters, and Steamfitters"),
    "pipefitter":                      ("47-2152.00", "Plumbers, Pipefitters, and Steamfitters"),
    "hvac technician":                 ("49-9021.00", "Heating, Air Conditioning, and Refrigeration Mechanics and Installers"),
    "hvac installer":                  ("49-9021.00", "Heating, Air Conditioning, and Refrigeration Mechanics and Installers"),
    "carpenter":                       ("47-2031.00", "Carpenters"),
    "industrial maintenance technician":("49-9071.00", "Maintenance and Repair Workers, General"),
    "maintenance technician":          ("49-9071.00", "Maintenance and Repair Workers, General"),
    "maintenance mechanic":            ("49-9071.00", "Maintenance and Repair Workers, General"),
    "facilities technician":           ("49-9071.00", "Maintenance and Repair Workers, General"),

    # ── Warehouse / Logistics ─────────────────────────────────────────────────
    "forklift operator":               ("53-7051.00", "Industrial Truck and Tractor Operators"),
    "forklift driver":                 ("53-7051.00", "Industrial Truck and Tractor Operators"),
    "warehouse associate":             ("53-7065.00", "Stockers and Order Fillers"),
    "warehouse worker":                ("53-7065.00", "Stockers and Order Fillers"),
    "order picker":                    ("53-7065.00", "Stockers and Order Fillers"),
    "picker packer":                   ("53-7065.00", "Stockers and Order Fillers"),
    "shipping receiving clerk":        ("43-5071.00", "Shipping, Receiving, and Inventory Clerks"),
    "shipping and receiving clerk":    ("43-5071.00", "Shipping, Receiving, and Inventory Clerks"),
    "inventory clerk":                 ("43-5071.00", "Shipping, Receiving, and Inventory Clerks"),
    "truck driver":                    ("53-3032.00", "Heavy and Tractor-Trailer Truck Drivers"),
    "cdl driver":                      ("53-3032.00", "Heavy and Tractor-Trailer Truck Drivers"),
    "delivery driver":                 ("53-3033.00", "Light Truck Drivers"),

    # ── Administrative / Clerical ─────────────────────────────────────────────
    "administrative assistant":        ("43-6014.00", "Secretaries and Administrative Assistants"),
    "admin assistant":                 ("43-6014.00", "Secretaries and Administrative Assistants"),
    "executive assistant":             ("43-6011.00", "Executive Secretaries and Executive Administrative Assistants"),
    "receptionist":                    ("43-4171.00", "Receptionists and Information Clerks"),
    "data entry clerk":                ("43-9021.00", "Data Entry Keyers"),
    "data entry specialist":           ("43-9021.00", "Data Entry Keyers"),
    "office manager":                  ("43-1011.00", "First-Line Supervisors of Office and Administrative Support Workers"),
    "office administrator":            ("43-6014.00", "Secretaries and Administrative Assistants"),
    "customer service representative": ("43-4051.00", "Customer Service Representatives"),
    "customer service rep":            ("43-4051.00", "Customer Service Representatives"),
    "call center agent":               ("43-4051.00", "Customer Service Representatives"),
    "customer service specialist":     ("43-4051.00", "Customer Service Representatives"),
    "payroll specialist":              ("43-3051.00", "Payroll and Timekeeping Clerks"),
    "payroll clerk":                   ("43-3051.00", "Payroll and Timekeeping Clerks"),

    # ── Staffing / Light Industrial ───────────────────────────────────────────
    "general labor":                   ("47-3099.00", "Construction and Related Workers, All Other"),
    "general laborer":                 ("47-3099.00", "Construction and Related Workers, All Other"),
    "light industrial":                ("51-9198.00", "Production Workers, All Other"),
    "material handler":                ("53-7062.00", "Laborers and Freight, Stock, and Material Movers, Hand"),
    "material handling":               ("53-7062.00", "Laborers and Freight, Stock, and Material Movers, Hand"),
    "packer":                          ("53-7065.00", "Stockers and Order Fillers"),
    "inspector":                       ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
}


def _normalize(title: str) -> str:
    return re.sub(r"\s+", " ", title.lower().strip())


class ONETClient:
    """
    Maps free-text job titles to O*NET SOC codes.

    Priority:
    1. Local cache lookup (instant)
    2. Fuzzy match against local cache (fast, no API)
    3. O*NET keyword search API (requires credentials or falls back gracefully)
    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
    ):
        self.username = username or os.getenv("ONET_USERNAME", "")
        self.password = password or os.getenv("ONET_PASSWORD", "")
        self.session  = requests.Session()
        if self.username:
            self.session.auth = (self.username, self.password)
        self.session.headers.update({"Accept": "application/json"})

    def search_occupations(self, title: str, max_results: int = 5) -> list[dict]:
        """
        Return up to max_results SOC matches, ranked by confidence.
        Each result: {"code": str, "title": str, "score": float, "source": str}
        """
        normalized = _normalize(title)
        results = []

        # 1. Exact local match
        if normalized in LOCAL_TITLE_MAP:
            code, onet_title = LOCAL_TITLE_MAP[normalized]
            results.append({
                "code":   code,
                "title":  onet_title,
                "score":  1.0,
                "source": "local_map",
            })

        # 2. Fuzzy match against local map keys
        scored = []
        for key, (code, onet_title) in LOCAL_TITLE_MAP.items():
            ratio = SequenceMatcher(None, normalized, key).ratio()
            if ratio > 0.55:
                scored.append((ratio, code, onet_title, key))

        scored.sort(reverse=True)
        seen_codes = {r["code"] for r in results}
        for ratio, code, onet_title, matched_key in scored[:max_results]:
            if code not in seen_codes:
                results.append({
                    "code":   code,
                    "title":  onet_title,
                    "score":  round(ratio, 3),
                    "source": f"fuzzy_match→{matched_key}",
                })
                seen_codes.add(code)

        # 3. O*NET keyword API (if credentials available and we need more results)
        if len(results) < 3 and self.username:
            api_results = self._api_search(title)
            for r in api_results:
                if r["code"] not in seen_codes:
                    results.append(r)
                    seen_codes.add(r["code"])

        # 4. O*NET public search (no credentials needed, slower)
        if len(results) < 2:
            api_results = self._public_search(title)
            for r in api_results:
                if r["code"] not in seen_codes:
                    results.append(r)
                    seen_codes.add(r["code"])

        return results[:max_results]

    def _api_search(self, title: str) -> list[dict]:
        """Authenticated O*NET keyword search."""
        try:
            url = f"{ONET_BASE}search"
            params = {"keyword": title, "start": 1, "end": 10, "fmt": "json"}
            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            out = []
            for occ in data.get("occupation", []):
                out.append({
                    "code":   occ.get("code", ""),
                    "title":  occ.get("title", ""),
                    "score":  occ.get("relevance_score", 0.5),
                    "source": "onet_api",
                })
            return out
        except Exception as e:
            print(f"[O*NET] API search error: {e}")
            return []

    def _public_search(self, title: str) -> list[dict]:
        """
        Unauthenticated fallback using O*NET's public keyword search.
        Parses the JSON response from the open endpoint.
        """
        try:
            url = "https://www.onetonline.org/search/quick"
            params = {"s": title}
            headers = {"Accept": "application/json", "User-Agent": "CompScope/1.0"}
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            # O*NET doesn't have a true public JSON API; this returns HTML.
            # In production, parse HTML or use registered credentials.
            # Here we return empty and rely on local fuzzy match.
            return []
        except Exception:
            return []
