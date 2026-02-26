"""
CompScope — Multi-Source Compensation Research Tool
====================================================
US-only. Pulls BLS OEWS, JSearch live postings, and USAJobs
at metro -> state -> national levels. Shows each source separately
then blends all available medians into one estimate.

Run locally:  streamlit run app.py
"""

import os
import statistics
import streamlit as st
import pandas as pd

# Load Streamlit secrets into env vars
def _load_secrets():
    for k in ["BLS_API_KEY","USAJOBS_API_KEY","USAJOBS_USER_AGENT",
              "JSEARCH_API_KEY","ONET_USERNAME","ONET_PASSWORD"]:
        if k not in os.environ:
            try:
                os.environ[k] = st.secrets[k]
            except Exception:
                pass

_load_secrets()

from data_sources.bls     import BLSClient
from data_sources.onet    import ONETClient
from data_sources.jsearch import JSearchClient
from data_sources.usajobs import USAJobsClient
from utils.geo            import resolve_msa, STATE_NAMES, STATE_FIPS

st.set_page_config(page_title="CompScope", page_icon="💰", layout="wide",
                   initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Mono:wght@400;500&display=swap');
:root { --ink:#0f0e0d; --paper:#faf8f4; --accent:#c84b1f; --muted:#7a7368;
        --border:#e2ddd6; --green:#2d6a4f; --amber:#e07b2a; }
html,body,[class*="css"] { background:var(--paper); color:var(--ink); }
h1,h2,h3 { font-family:'DM Serif Display',serif; }
.hero-title { font-family:'DM Serif Display',serif; font-size:3rem; line-height:1.1; }
.hero-sub { font-size:1rem; color:var(--muted); margin-bottom:2rem; }
.card { background:white; border:1px solid var(--border); border-radius:6px;
        padding:1.2rem 1.5rem; margin-bottom:1rem; }
.src-header { font-family:'DM Serif Display',serif; font-size:1.1rem; margin:0; }
.geo-row { display:flex; align-items:baseline; gap:1.5rem; flex-wrap:wrap;
           padding:0.45rem 0; border-bottom:1px solid var(--border); }
.geo-row:last-child { border-bottom:none; }
.geo-label { font-family:'DM Mono',monospace; font-size:0.78rem; color:var(--muted); min-width:230px; }
.geo-median { font-family:'DM Mono',monospace; font-size:1.05rem; font-weight:600; color:var(--ink); }
.geo-range { font-family:'DM Mono',monospace; font-size:0.82rem; color:var(--muted); }
.src-median { font-family:'DM Mono',monospace; font-size:0.85rem; color:var(--green); margin-top:0.5rem; }
.badge { display:inline-block; font-family:'DM Mono',monospace; font-size:0.68rem;
         padding:2px 7px; border-radius:2px; margin-left:6px; vertical-align:middle; }
.b-metro { background:#d1fae5; color:#065f46; }
.b-state { background:#fef3c7; color:#92400e; }
.b-national { background:#fee2e2; color:#991b1b; }
.b-live { background:#dbeafe; color:#1e40af; }
.blend-box { border-left:4px solid var(--green); background:#f0fdf4;
             padding:1rem 1.5rem; border-radius:0 6px 6px 0; margin:1rem 0; }
.warn-box { border-left:3px solid var(--amber); background:#fffbf0;
            padding:0.7rem 1rem; font-size:0.85rem; color:#6b4a0a;
            border-radius:0 4px 4px 0; margin:0.8rem 0; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_clients():
    return BLSClient(), ONETClient(), JSearchClient(), USAJobsClient()

bls, onet, jsearch, usajobs = get_clients()

def fmt(val):
    try:
        return f"${float(str(val).replace(',','').replace('$','')):,.0f}"
    except Exception:
        return "—"

def safe_float(val):
    try:
        return float(str(val).replace(",","").replace("$",""))
    except Exception:
        return None

def geo_row(icon, label, median, low=None, high=None):
    range_str = f"<span class='geo-range'>{fmt(low)} – {fmt(high)}</span>" if low and high else ""
    return f"""<div class="geo-row">
      <span class="geo-label">{icon} {label}</span>
      <span class="geo-median">{fmt(median)}</span>
      {range_str}
    </div>"""

# Hero
st.markdown('<div class="hero-title">CompScope</div>', unsafe_allow_html=True)
st.markdown('<div class="hero-sub">US compensation intelligence — metro, state, and national from BLS, live postings, and federal data.</div>', unsafe_allow_html=True)

# Input
c1, c2, c3 = st.columns([3, 2, 1])
with c1: job_title = st.text_input("Job title", placeholder="e.g. Senior Data Engineer")
with c2: location  = st.text_input("City, State", placeholder="e.g. Austin, TX")
with c3:
    st.write(""); st.write("")
    run_btn = st.button("Research →", use_container_width=True)

if run_btn and job_title and location:

    with st.spinner("Resolving geography…"):
        geo = resolve_msa(location)

    with st.spinner("Matching job title to SOC code…"):
        soc_matches = onet.search_occupations(job_title)

    if not soc_matches:
        st.error("No SOC code match found. Try a more standard job title.")
        st.stop()

    soc_options = {f"{m['title']} ({m['code']})": m for m in soc_matches[:5]}
    chosen = soc_options[st.selectbox("Best SOC match — confirm or choose another:", list(soc_options.keys()))]
    soc_code  = chosen["code"]
    soc_title = chosen["title"]

    st.markdown(f"**SOC:** `{soc_code}` · {soc_title}")
    st.divider()

    # Parse location for JSearch geo-level queries
    parts      = [p.strip() for p in location.rsplit(",", 1)]
    city       = parts[0] if parts else location
    state_abbr = parts[1].upper()[:2] if len(parts) > 1 else ""
    state_fips = STATE_FIPS.get(state_abbr, "")
    state_name = STATE_NAMES.get(state_fips, state_abbr)

    # local_medians = metro + state only (used for blended estimate)
    # natl_medians  = national only (shown as reference, not blended unless nothing local)
    local_medians = []
    natl_medians  = []

    # ── SOURCE 1: BLS OEWS ────────────────────────────────────────────────────
    bls_rows = {}
    with st.spinner("Querying BLS OEWS (US government wage survey)…"):
        if geo.get("msa_code"):
            d = bls.get_oews(soc_code, area_code=geo["msa_code"], area_type="MSA")
            if d: bls_rows["metro"] = {**d, "label": geo.get("msa_name", "Metro")}
        if geo.get("state_code"):
            d = bls.get_oews(soc_code, area_code=geo["state_code"], area_type="state")
            if d: bls_rows["state"] = {**d, "label": geo.get("state_name", "State")}
        d = bls.get_oews(soc_code, area_code="0000000", area_type="national")
        if d: bls_rows["national"] = {**d, "label": "United States (national reference)"}

    if bls_rows:
        html = ""
        bls_local = []
        for lvl, icon in [("metro","📍"),("state","🗺️"),("national","🌐")]:
            if lvl in bls_rows:
                d = bls_rows[lvl]
                m = safe_float(d.get("median"))
                if m:
                    if lvl == "national":
                        natl_medians.append(m)
                    else:
                        bls_local.append(m)
                        local_medians.append(m)
                html += geo_row(icon, d["label"], d.get("median"), d.get("pct25"), d.get("pct75"))
        yr = next(iter(bls_rows.values())).get("year","—")
        src_med = fmt(round(statistics.mean(bls_local))) if bls_local else "—"
        st.markdown(f"""<div class="card">
          <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.6rem;">
            <span class="src-header">📊 BLS OEWS — Base Wages</span>
            <span class="badge b-national">Survey year: {yr} · ⚠ 12-18 mo lag</span>
          </div>
          <div style="font-size:0.8rem;color:var(--muted);margin-bottom:0.6rem;">
            {soc_title} ({soc_code}) · Base wages only · Does not include bonus or equity
          </div>
          {html}
          <div class="src-median">Local/state median used in blend: {src_med}</div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown('<div class="card"><span class="src-header">📊 BLS OEWS</span> — No US data found for this role.</div>', unsafe_allow_html=True)

    # ── SOURCE 2: JSearch (live postings, US only) ────────────────────────────
    with st.spinner("Fetching live US postings (Indeed, LinkedIn, Glassdoor)…"):
        js_levels = jsearch.get_geo_levels(job_title, city, state_name)

    if js_levels:
        html = ""
        js_local = []
        for lvl, icon in [("metro","📍"),("state","🗺️"),("national","🌐")]:
            if lvl in js_levels:
                d = js_levels[lvl]
                m = safe_float(d.get("median"))
                if m:
                    if lvl == "national":
                        natl_medians.append(m)
                    else:
                        js_local.append(m)
                        local_medians.append(m)
                cnt = f" · {d['posting_count']} postings" if d.get("posting_count") else ""
                html += geo_row(icon, f"{d['geo_label']}{cnt}", d.get("median"), d.get("pct25"), d.get("pct75"))
        src_med = fmt(round(statistics.mean(js_local))) if js_local else "—"
        st.markdown(f"""<div class="card">
          <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.6rem;">
            <span class="src-header">📡 Live Job Postings</span>
            <span class="badge b-live">Real-time · US only</span>
          </div>
          <div style="font-size:0.8rem;color:var(--muted);margin-bottom:0.6rem;">
            Aggregated from Indeed, LinkedIn, Glassdoor, Google Jobs
          </div>
          {html}
          <div class="src-median">Local/state median used in blend: {src_med}</div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown('<div class="card"><span class="src-header">📡 Live Job Postings</span> — No US results returned (API key may not be configured).</div>', unsafe_allow_html=True)

    # ── SOURCE 3: USAJobs (federal only — reference, NOT blended) ─────────────
    with st.spinner("Scanning USAJobs federal postings…"):
        uj_local    = usajobs.search(job_title, location)
        uj_national = usajobs.search(job_title, "")

    def uj_stats(posts):
        mins = [j["salary_min"] for j in posts if j.get("salary_min")]
        maxs = [j["salary_max"] for j in posts if j.get("salary_max")]
        if not mins: return None
        return {"median": round(statistics.median(mins+maxs)),
                "min": round(min(mins)), "max": round(max(maxs)) if maxs else None,
                "count": len(posts)}

    us_local = uj_stats(uj_local)    if uj_local    else None
    us_natl  = uj_stats(uj_national) if uj_national else None

    if us_local or us_natl:
        html = ""
        if us_local:
            html += geo_row("📍", f"{location} ({us_local['count']} federal postings)",
                            us_local.get("median"), us_local.get("min"), us_local.get("max"))
        if us_natl:
            html += geo_row("🌐", f"United States ({us_natl['count']} federal postings)",
                            us_natl.get("median"), us_natl.get("min"), us_natl.get("max"))
        st.markdown(f"""<div class="card">
          <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.6rem;">
            <span class="src-header">🏛 USAJobs — Federal Sector Reference</span>
            <span class="badge b-state">Federal GS-scale only</span>
          </div>
          <div style="font-size:0.8rem;color:var(--muted);margin-bottom:0.4rem;">
            Federal salaries only — not included in private-sector blend
          </div>
          <div class="warn-box" style="margin:0.4rem 0 0.6rem 0;font-size:0.8rem;">
            ⚠ GS-scale federal pay is typically <strong>20–40% below private market</strong> for most roles.
            Use for public-sector context only.
          </div>
          {html}
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown('<div class="card"><span class="src-header">🏛 USAJobs</span> — No federal postings found.</div>', unsafe_allow_html=True)

    # ── BLENDED ESTIMATE (metro + state only; national as fallback) ───────────
    blend_medians = local_medians if local_medians else natl_medians
    if blend_medians:
        blended = round(statistics.mean(blend_medians))
        geo_scope = "metro + state data" if local_medians else "national data only (no local data available)"
        st.markdown(f"""<div class="blend-box">
          <div style="font-size:0.85rem;color:var(--muted);margin-bottom:0.2rem;">
            Private-Sector Blended Estimate — {geo_scope}
          </div>
          <div style="font-family:'DM Mono',monospace;font-size:2.2rem;font-weight:700;
                      color:var(--green);line-height:1.2;">{fmt(blended)}</div>
          <div style="font-size:0.85rem;color:var(--muted);margin-top:0.3rem;">
            median annual base &nbsp;·&nbsp;
            range: {fmt(round(min(blend_medians)))} – {fmt(round(max(blend_medians)))}
            &nbsp;·&nbsp; {len(blend_medians)} source(s)
          </div>
        </div>""", unsafe_allow_html=True)

    st.markdown("""<div class="warn-box">
    All figures reflect <strong>US-only, base wages</strong>. Bonus, equity (RSUs/options), and benefits
    are excluded. For tech roles, total comp commonly exceeds base by 20–60%+.
    BLS data lags 12-18 months; live postings reflect today's market.
    </div>""", unsafe_allow_html=True)

    with st.expander("Data sources & methodology"):
        st.markdown("""
**BLS OEWS** — Annual US employer survey, ~1.1M establishments. Shows metro, state, and national rows.
Only metro + state rows feed the blended estimate; national is shown as a reference anchor.

**JSearch** — Real-time US postings from Indeed, LinkedIn, Glassdoor, ZipRecruiter.
Queried separately at metro, state, and national levels. Only metro + state feed the blend.

**USAJobs** — US federal government postings only. **Not included in the private-sector blend.**
Federal GS-scale pay is typically 20–40% below private market for most professional roles.

**Blended estimate** — Mean of available metro- and state-level medians from BLS + JSearch.
Falls back to national data only if no local/state data is returned.

**O*NET** — Maps free-text job titles to 6-digit SOC codes used by BLS for lookups.
        """)

elif run_btn:
    st.warning("Please enter both a job title and location.")
