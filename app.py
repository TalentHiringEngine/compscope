"""
CompScope — Local Market Compensation Research Tool
====================================================
Streamlit app that queries BLS OEWS (free), O*NET (free), and
USAJobs (free) to return geographically-specific pay data.

Run:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import json
import time
from data_sources.bls import BLSClient
from onet import ONETClient
from data_sources.usajobs import USAJobsClient
from data_sources.jsearch import JSearchClient
from adzuna import AdzunaClient
from utils.geo import resolve_msa
from utils.soc import SOCMapper

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CompScope",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&display=swap');

:root {
    --ink:     #0f0e0d;
    --paper:   #faf8f4;
    --accent:  #c84b1f;
    --muted:   #7a7368;
    --border:  #e2ddd6;
    --green:   #2d6a4f;
    --amber:   #e07b2a;
}

html, body, [class*="css"] { background: var(--paper); color: var(--ink); }

h1, h2, h3 { font-family: 'DM Serif Display', serif; }
code, .mono { font-family: 'DM Mono', monospace; }

.hero-title {
    font-family: 'DM Serif Display', serif;
    font-size: 3.2rem;
    line-height: 1.1;
    color: var(--ink);
    margin-bottom: 0.2rem;
}
.hero-sub {
    font-size: 1.05rem;
    color: var(--muted);
    margin-bottom: 2rem;
}

.card {
    background: white;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1rem;
}

.geo-badge {
    display: inline-block;
    font-family: 'DM Mono', monospace;
    font-size: 0.72rem;
    padding: 2px 8px;
    border-radius: 2px;
    margin-left: 8px;
    vertical-align: middle;
}
.geo-metro   { background: #d1fae5; color: #065f46; }
.geo-state   { background: #fef3c7; color: #92400e; }
.geo-national{ background: #fee2e2; color: #991b1b; }

.pct-bar-wrap { margin: 1rem 0; }
.pct-label { font-family: 'DM Mono', monospace; font-size: 0.78rem; color: var(--muted); }
.pct-value { font-family: 'DM Mono', monospace; font-size: 1.1rem; font-weight: 500; }
.pct-median { color: var(--accent); font-size: 1.3rem !important; }

.source-chip {
    display: inline-block;
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    background: var(--border);
    color: var(--muted);
    padding: 2px 7px;
    border-radius: 2px;
    margin: 2px;
}

.warn-box {
    border-left: 3px solid var(--amber);
    background: #fffbf0;
    padding: 0.8rem 1rem;
    font-size: 0.88rem;
    color: #6b4a0a;
    border-radius: 0 4px 4px 0;
    margin: 0.5rem 0;
}

stButton>button {
    background: var(--accent) !important;
    color: white !important;
    border: none !important;
    font-family: 'DM Mono', monospace !important;
    letter-spacing: 0.05em;
}
</style>
""", unsafe_allow_html=True)

# ── Clients (cached) ───────────────────────────────────────────────────────────
@st.cache_resource
def get_clients():
    jsearch_key = st.secrets.get("JSEARCH_API_KEY", "fdc069935fmshf484da2c3899b89p1f0e18jsn0b030ebcc722")
    return BLSClient(), ONETClient(), USAJobsClient(), JSearchClient(api_key=jsearch_key), AdzunaClient(), SOCMapper()

bls, onet, usajobs, jsearch, adzuna, soc_mapper = get_clients()

# ── Hero ───────────────────────────────────────────────────────────────────────
st.markdown('<div class="hero-title">CompScope</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-sub">Local-first compensation intelligence — '
    'metro data when available, state or national only as a fallback.</div>',
    unsafe_allow_html=True,
)

# ── Input form ─────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([3, 2, 1])
with col1:
    job_title = st.text_input("Job title", placeholder="e.g. Senior Data Engineer")
with col2:
    location = st.text_input("City, State", placeholder="e.g. Austin, TX")
with col3:
    st.write("")
    st.write("")
    run = st.button("Research →", use_container_width=True)

# ── Main logic ─────────────────────────────────────────────────────────────────
if run and job_title and location:

    with st.spinner("Resolving geography…"):
        geo = resolve_msa(location)

    with st.spinner("Matching job title to SOC code…"):
        soc_matches = onet.search_occupations(job_title)

    # Temporary debug — shows what the app is receiving; remove after confirming fix
    with st.expander("🔍 Debug info", expanded=False):
        st.write(f"Job title received: `{repr(job_title)}`")
        st.write(f"Normalized: `{repr(job_title.lower().strip())}`")
        st.write(f"SOC matches found: `{soc_matches}`")

    if not soc_matches:
        st.error(
            f"Could not find a matching SOC code for **{job_title!r}**. "
            "Try a standard title like 'Software Engineer', 'Data Analyst', or 'Registered Nurse'."
        )
        st.stop()

    # Let user pick the SOC match
    soc_options = {f"{m['title']} ({m['code']})": m for m in soc_matches[:5]}
    chosen_label = st.selectbox(
        "Best SOC match — confirm or choose another:",
        options=list(soc_options.keys()),
    )
    chosen = soc_options[chosen_label]
    soc_code = chosen["code"]
    soc_title = chosen["title"]

    st.markdown(f"**SOC Code:** `{soc_code}` · **Standard Title:** {soc_title}")
    st.divider()

    # ── BLS OEWS Query cascade ─────────────────────────────────────────────────
    results = {}

    if geo.get("msa_code"):
        with st.spinner(f"Querying BLS OEWS for {geo['msa_name']}…"):
            data = bls.get_oews(soc_code, area_code=geo["msa_code"], area_type="MSA")
            if data:
                results["bls_oews"] = {**data, "geo_level": "metro", "geo_name": geo["msa_name"]}

    if "bls_oews" not in results and geo.get("state_code"):
        with st.spinner(f"Metro data unavailable — trying state ({geo['state_name']})…"):
            data = bls.get_oews(soc_code, area_code=geo["state_code"], area_type="state")
            if data:
                results["bls_oews"] = {**data, "geo_level": "state", "geo_name": geo["state_name"]}

    if "bls_oews" not in results:
        with st.spinner("Falling back to national BLS data…"):
            data = bls.get_oews(soc_code, area_code="0000000", area_type="national")
            if data:
                results["bls_oews"] = {**data, "geo_level": "national", "geo_name": "National"}

    # ── BLS broader SOC fallback (for thin/missing occupations) ───────────────
    if "bls_oews" not in results:
        for broader_soc in soc_mapper.fallback_chain(soc_code):
            with st.spinner(f"BLS data sparse for this occupation — trying broader group ({broader_soc})…"):
                data = bls.get_oews(broader_soc, area_code="0000000", area_type="national")
                if data:
                    results["bls_oews"] = {**data, "geo_level": "national", "geo_name": "National (occupation group)"}
                    st.info(f"No BLS data for exact occupation — showing data for the broader **{soc_mapper.describe(broader_soc)}** group as a reference.")
                    break

    # ── USAJobs ────────────────────────────────────────────────────────────────
    with st.spinner("Scanning USAJobs postings for salary ranges…"):
        postings = usajobs.search(job_title, location)
        if postings:
            results["usajobs"] = postings

    # ── JSearch (Indeed + LinkedIn + Glassdoor) ────────────────────────────────
    with st.spinner("Scanning Indeed, LinkedIn & Glassdoor for live salary data…"):
        city_state = location
        state_name = geo.get("state_name", location)
        city_name  = location.split(",")[0].strip()
        jsearch_data = jsearch.get_geo_levels(job_title, city_name, state_name)
        if jsearch_data:
            results["jsearch"] = jsearch_data
        jsearch_postings = jsearch.get_sample_postings(job_title, city_state)
        if jsearch_postings:
            results["jsearch_postings"] = jsearch_postings

    # ── Adzuna job postings ────────────────────────────────────────────────────
    with st.spinner("Scanning live job postings for salary ranges (Adzuna)…"):
        adzuna_data = adzuna.search(job_title, location)
        if adzuna_data:
            results["adzuna"] = adzuna_data

    # ── Render ─────────────────────────────────────────────────────────────────
    if not results:
        st.warning("No compensation data found. Try a different title or location.")
        st.stop()

    # BLS OEWS card
    if "bls_oews" in results:
        d = results["bls_oews"]
        geo_class = {"metro": "geo-metro", "state": "geo-state", "national": "geo-national"}[d["geo_level"]]
        geo_label = {"metro": "📍 Metro", "state": "🗺 State", "national": "🌐 National"}[d["geo_level"]]

        st.markdown(f"""
        <div class="card">
          <div style="display:flex;align-items:baseline;gap:0.5rem;margin-bottom:0.8rem;">
            <h3 style="margin:0;">BLS OEWS Wage Survey</h3>
            <span class="geo-badge {geo_class}">{geo_label}: {d['geo_name']}</span>
          </div>
          <div style="color:var(--muted);font-size:0.82rem;margin-bottom:1rem;">
            Survey year: {d.get('year','—')} &nbsp;·&nbsp;
            Occupation: {soc_title} ({soc_code}) &nbsp;·&nbsp;
            n={d.get('employment','—')} workers surveyed
          </div>
        """, unsafe_allow_html=True)

        pcts = [
            ("10th", d.get("pct10")),
            ("25th", d.get("pct25")),
            ("50th Median", d.get("median"), True),
            ("75th", d.get("pct75")),
            ("90th", d.get("pct90")),
        ]

        cols = st.columns(5)
        for i, pct_info in enumerate(pcts):
            label = pct_info[0]
            val   = pct_info[1]
            is_med = len(pct_info) > 2

            with cols[i]:
                if val:
                    try:
                        formatted = f"${float(val.replace(',','').replace('$','')):,.0f}"
                    except Exception:
                        formatted = val
                    color = "var(--accent)" if is_med else "var(--ink)"
                    size  = "1.4rem" if is_med else "1.1rem"
                    st.markdown(
                        f'<div class="pct-label">{label}</div>'
                        f'<div style="font-family:DM Mono,monospace;font-size:{size};color:{color};font-weight:500;">{formatted}</div>',
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(f'<div class="pct-label">{label}</div><div style="color:var(--muted);">—</div>', unsafe_allow_html=True)

        # Bonus note
        st.markdown("""
        <div style="margin-top:1rem;font-size:0.82rem;color:var(--muted);">
        ⚠ BLS OEWS reflects <strong>base wages only</strong> — does not include bonus, equity, or benefits.
        Annual figures shown; hourly × 2,080 where applicable.
        </div></div>""", unsafe_allow_html=True)

    # USAJobs card
    if "usajobs" in results:
        postings = results["usajobs"]
        st.markdown(f"""
        <div class="card">
          <div style="display:flex;align-items:baseline;gap:0.5rem;margin-bottom:0.8rem;">
            <h3 style="margin:0;">🏛 USAJobs — Federal Government Postings Only</h3>
            <span class="geo-badge geo-national">⚠ Federal sector</span>
          </div>
          <div style="color:var(--muted);font-size:0.82rem;margin-bottom:0.5rem;">
            {len(postings)} active federal postings near {location} with salary data
          </div>
          <div class="warn-box" style="margin-bottom:1rem;">
            <strong>Note:</strong> Federal GS-scale salaries are typically <strong>20–40% below private-sector market rates</strong>
            for tech roles. Use this data for public-sector benchmarking only — do not compare directly to private employer offers.
          </div>
        """, unsafe_allow_html=True)

        df = pd.DataFrame(postings)[["title", "salary_min", "salary_max", "pay_scale", "location", "url"]]
        df["salary_min"] = df["salary_min"].apply(lambda x: f"${x:,.0f}" if x else "—")
        df["salary_max"] = df["salary_max"].apply(lambda x: f"${x:,.0f}" if x else "—")
        df.columns = ["Title", "Min", "Max", "Pay Scale", "Location", "Link"]
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ── JSearch card ──────────────────────────────────────────────────────────
    if "jsearch" in results:
        js = results["jsearch"]
        for level, label, badge in [("metro","📍 Metro","geo-metro"),("state","🗺 State","geo-state"),("national","🌐 National","geo-national")]:
            if level not in js:
                continue
            d = js[level]
            hourly_med = round(d["median"] / 2080) if d.get("median") else None
            st.markdown(f"""
            <div class="card">
              <div style="display:flex;align-items:baseline;gap:0.5rem;margin-bottom:0.8rem;">
                <h3 style="margin:0;">Indeed · LinkedIn · Glassdoor</h3>
                <span class="geo-badge {badge}">{label}: {d.get('geo_label','')}</span>
                <span class="source-chip">🔴 Live postings</span>
              </div>
              <div style="color:var(--muted);font-size:0.82rem;margin-bottom:1rem;">
                Based on {d.get('posting_count','?')} postings with disclosed salary data
              </div>
            """, unsafe_allow_html=True)

            cols = st.columns(4)
            with cols[0]:
                st.markdown('<div class="pct-label">Average Salary</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="font-family:DM Mono,monospace;font-size:1.4rem;color:var(--accent);font-weight:500;">${d["median"]:,.0f}</div>', unsafe_allow_html=True)
            with cols[1]:
                lo = d.get("min") or d.get("pct25")
                hi = d.get("max") or d.get("pct75")
                st.markdown('<div class="pct-label">Salary Range</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="font-family:DM Mono,monospace;font-size:1.1rem;font-weight:500;">${lo:,.0f} – ${hi:,.0f}</div>' if lo and hi else '<div>—</div>', unsafe_allow_html=True)
            with cols[2]:
                st.markdown('<div class="pct-label">25th / 75th</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="font-family:DM Mono,monospace;font-size:1.1rem;font-weight:500;">${d.get("pct25",0):,.0f} / ${d.get("pct75",0):,.0f}</div>', unsafe_allow_html=True)
            with cols[3]:
                st.markdown('<div class="pct-label">Hourly Rate</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="font-family:DM Mono,monospace;font-size:1.1rem;font-weight:500;">${hourly_med}/hr</div>' if hourly_med else '<div>—</div>', unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)
            break  # show most specific geo level only

    if "jsearch_postings" in results and results["jsearch_postings"]:
        with st.expander(f"🏢 Companies actively hiring ({len(results['jsearch_postings'])} postings)"):
            df = pd.DataFrame(results["jsearch_postings"])[["title","employer","location","salary_min","salary_max","posted","url"]]
            df["salary_min"] = df["salary_min"].apply(lambda x: f"${x:,.0f}" if x else "—")
            df["salary_max"] = df["salary_max"].apply(lambda x: f"${x:,.0f}" if x else "—")
            df.columns = ["Title","Company","Location","Min","Max","Posted","Link"]
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Adzuna job postings card ───────────────────────────────────────────────
    if "adzuna" in results:
        az = results["adzuna"]
        st.markdown(f"""
        <div class="card">
          <div style="display:flex;align-items:baseline;gap:0.5rem;margin-bottom:0.8rem;">
            <h3 style="margin:0;">📋 Live Job Postings — Market Salary Ranges</h3>
            <span class="geo-badge geo-metro">🔴 Live</span>
          </div>
          <div style="color:var(--muted);font-size:0.82rem;margin-bottom:1rem;">
            Aggregated from {az['count']} active postings near {location} with salary data
            &nbsp;·&nbsp; Source: Adzuna (Indeed, CareerBuilder + others)
          </div>
        """, unsafe_allow_html=True)

        az_pcts = [
            ("25th", az.get("pct25")),
            ("50th Median", az.get("median"), True),
            ("75th", az.get("pct75")),
        ]
        cols = st.columns(3)
        for i, pct_info in enumerate(az_pcts):
            label  = pct_info[0]
            val    = pct_info[1]
            is_med = len(pct_info) > 2
            with cols[i]:
                if val:
                    color = "var(--accent)" if is_med else "var(--ink)"
                    size  = "1.4rem" if is_med else "1.1rem"
                    st.markdown(
                        f'<div class="pct-label">{label}</div>'
                        f'<div style="font-family:DM Mono,monospace;font-size:{size};color:{color};font-weight:500;">${val:,.0f}</div>',
                        unsafe_allow_html=True
                    )

        if az.get("postings"):
            with st.expander(f"View {len(az['postings'])} sample postings"):
                import pandas as pd
                df = pd.DataFrame(az["postings"])[["title", "company", "salary_min", "salary_max", "location", "created", "url"]]
                df["salary_min"] = df["salary_min"].apply(lambda x: f"${x:,.0f}" if x else "—")
                df["salary_max"] = df["salary_max"].apply(lambda x: f"${x:,.0f}" if x else "—")
                df.columns = ["Title", "Company", "Min", "Max", "Location", "Posted", "Link"]
                st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown("""
        <div style="margin-top:0.8rem;font-size:0.82rem;color:var(--muted);">
        ⚠ Posting salary ranges reflect <strong>what employers advertise</strong>, which may be
        broader than actual offers. Median of posting midpoints shown.
        </div></div>""", unsafe_allow_html=True)

    elif adzuna.app_id:
        pass  # JSearch is primary source; suppress redundant Adzuna warning

    # ── Context note ───────────────────────────────────────────────────────────
    st.markdown("""
    <div class="warn-box">
    <strong>Why BLS data may read low:</strong> BLS OEWS captures <strong>base wages only</strong>
    and is 12–18 months old. It does not include bonus, equity (RSUs/options), benefits, or
    signing bonuses — which in tech can add 20–50% to total comp. For roles like Senior Data Engineer,
    total compensation packages at mid-to-large tech companies commonly run $160K–$220K+ in
    major metros, well above the BLS base median. BLS remains the gold standard for <em>base salary</em>
    market benchmarks across all employers.
    </div>
    """, unsafe_allow_html=True)

    # ── Source provenance ──────────────────────────────────────────────────────
    with st.expander("Data provenance & methodology"):
        st.markdown("""
**BLS OEWS** — Occupational Employment and Wage Statistics program. Annual employer survey
covering ~1.1M establishments. Metro-area (MSA/NECTA) data published each April.
[docs.bls.gov/api](https://www.bls.gov/developers/)

**O*NET** — Standard Occupational Classification search API. Used to map free-text
job titles to 6-digit SOC codes. [onetonline.org](https://www.onetonline.org/)

**USAJobs** — Federal government job postings API. All positions include pay band data.
Useful for public-sector benchmarking. [developer.usajobs.gov](https://developer.usajobs.gov/)

**Geography resolution** — City/state input is matched to Census CBSA codes, then
mapped to BLS OEWS area codes. Metro → State → National fallback chain applied automatically.

**Not yet integrated (paid/scraping)**:
- Payscale Insight Lab API (~$5k/yr)
- Mercer/iMercer CompAnalyst
- Lightcast (formerly EMSI Burning Glass) — MSA-level real-time posting analytics
- Levels.fyi — scraping possible for TC data in tech roles
- Glassdoor Employer API — requires partnership
- LinkedIn Salary Insights API — enterprise only
        """)

elif run:
    st.warning("Please enter both a job title and location.")
