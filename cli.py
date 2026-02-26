#!/usr/bin/env python3
"""
CompScope CLI
=============
Command-line version of the compensation research tool.
Usage: python cli.py "Senior Data Engineer" "Austin, TX"
       python cli.py "Registered Nurse" "Seattle, WA" --verbose
"""

import argparse
import sys
import json

# Make relative imports work when run directly
import os
sys.path.insert(0, os.path.dirname(__file__))

from data_sources.bls  import BLSClient
from data_sources.onet import ONETClient
from utils.geo         import resolve_msa
from utils.soc         import SOCMapper


def fmt_currency(val):
    if val is None:
        return "—"
    try:
        return f"${float(str(val).replace(',','').replace('$','')):>10,.0f}"
    except Exception:
        return str(val)


def run(job_title: str, location: str, verbose: bool = False, json_output: bool = False):
    bls  = BLSClient()
    onet = ONETClient()
    soc  = SOCMapper()

    print(f"\n🔍 Researching: {job_title!r} in {location!r}\n")

    # ── Geo resolution ──────────────────────────────────────────────────────────
    geo = resolve_msa(location)
    if verbose:
        print(f"[geo] MSA={geo.get('msa_name')} ({geo.get('msa_code')})  "
              f"State={geo.get('state_name')} ({geo.get('state_code')})")

    # ── SOC matching ────────────────────────────────────────────────────────────
    matches = onet.search_occupations(job_title)
    if not matches:
        print("❌  No SOC code match found. Try a more common job title.")
        return

    best      = matches[0]
    soc_code  = best["code"]
    soc_title = best["title"]

    print(f"SOC Match:  {soc_title} ({soc_code})")
    if len(matches) > 1 and verbose:
        print("Other matches:")
        for m in matches[1:]:
            print(f"  {m['title']} ({m['code']})  score={m['score']:.2f}")
    print()

    # ── BLS OEWS cascade ────────────────────────────────────────────────────────
    data = None
    geo_level = None
    geo_name  = None

    if geo.get("msa_code"):
        data = bls.get_oews(soc_code, area_code=geo["msa_code"], area_type="MSA")
        if data:
            geo_level = "Metro"
            geo_name  = geo["msa_name"]

    if not data and geo.get("state_code"):
        data = bls.get_oews(soc_code, area_code=geo["state_code"], area_type="state")
        if data:
            geo_level = "State"
            geo_name  = geo["state_name"]

    if not data:
        data = bls.get_oews(soc_code, area_code="0000000", area_type="national")
        if data:
            geo_level = "National"
            geo_name  = "United States"

    # ── Output ──────────────────────────────────────────────────────────────────
    if json_output:
        result = {
            "job_title_input": job_title,
            "soc_code":        soc_code,
            "soc_title":       soc_title,
            "location_input":  location,
            "geo_level":       geo_level,
            "geo_name":        geo_name,
            "wages":           data,
        }
        print(json.dumps(result, indent=2))
        return

    if not data:
        print("❌  No BLS OEWS data found for this occupation + geography.")
        return

    geo_icon = {"Metro": "📍", "State": "🗺", "National": "🌐"}.get(geo_level, "")
    print(f"{'─'*52}")
    print(f"  BLS OEWS Annual Wages — {geo_icon} {geo_level}: {geo_name}")
    print(f"  Occupation: {soc_title} ({soc_code})")
    print(f"  Survey Year: {data.get('year','—')}  |  Employment: {data.get('employment','—')}")
    print(f"{'─'*52}")
    print(f"  {'10th percentile':25s}  {fmt_currency(data.get('pct10'))}")
    print(f"  {'25th percentile':25s}  {fmt_currency(data.get('pct25'))}")
    print(f"  {'50th (Median)':25s}  {fmt_currency(data.get('median'))}  ◀")
    print(f"  {'75th percentile':25s}  {fmt_currency(data.get('pct75'))}")
    print(f"  {'90th percentile':25s}  {fmt_currency(data.get('pct90'))}")
    print(f"  {'Annual Mean':25s}  {fmt_currency(data.get('mean'))}")
    print(f"{'─'*52}")
    print()

    if geo_level != "Metro":
        print(f"⚠  Metro-level data unavailable. Showing {geo_level}-level fallback.")
        print( "   Consider checking salary-transparent job postings (CA/CO/NY/WA) as")
        print( "   a real-time proxy for this market.")
        print()

    print("Sources: BLS OEWS (base wages only; excludes bonus, equity, benefits)")
    print("         O*NET Web Services (job title → SOC code mapping)")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CompScope — local comp research")
    parser.add_argument("job_title",  help='e.g. "Senior Data Engineer"')
    parser.add_argument("location",   help='e.g. "Austin, TX"')
    parser.add_argument("--verbose",  action="store_true")
    parser.add_argument("--json",     action="store_true", dest="json_output",
                        help="Output raw JSON instead of formatted table")
    args = parser.parse_args()
    run(args.job_title, args.location, args.verbose, args.json_output)
