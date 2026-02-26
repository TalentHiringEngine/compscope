"""
Geography Resolution
====================
Maps a "City, State" string to BLS-compatible area codes:
  - MSA/CBSA code (metro-level, most specific)
  - State FIPS code (fallback)

Uses the Census Geocoding API (free, no key) to resolve city → county → CBSA,
then maps CBSA to BLS OEWS area codes.

BLS area code conventions:
  National:  0000000
  State:     S{fips2}00000  e.g. S4800000 = Texas (FIPS 48)
  MSA:       M{cbsa5}00    e.g. M1242000 = Austin-Round Rock, TX (CBSA 12420)
  NECTA:     N{necta5}00   (New England only)
"""

import re
import requests
from functools import lru_cache
from typing import Optional


# Census Geocoding
CENSUS_GEOCODE_BASE = "https://geocoding.geo.census.gov/geocoder/locations/address"
CENSUS_CBSA_BASE    = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

# State name → FIPS lookup
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
    "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
    "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
    "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
    "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
    "DC": "11",
}

# State abbreviation reverse lookup for display
STATE_NAMES = {
    "01":"Alabama","02":"Alaska","04":"Arizona","05":"Arkansas","06":"California",
    "08":"Colorado","09":"Connecticut","10":"Delaware","11":"DC","12":"Florida",
    "13":"Georgia","15":"Hawaii","16":"Idaho","17":"Illinois","18":"Indiana",
    "19":"Iowa","20":"Kansas","21":"Kentucky","22":"Louisiana","23":"Maine",
    "24":"Maryland","25":"Massachusetts","26":"Michigan","27":"Minnesota",
    "28":"Mississippi","29":"Missouri","30":"Montana","31":"Nebraska","32":"Nevada",
    "33":"New Hampshire","34":"New Jersey","35":"New Mexico","36":"New York",
    "37":"North Carolina","38":"North Dakota","39":"Ohio","40":"Oklahoma",
    "41":"Oregon","42":"Pennsylvania","44":"Rhode Island","45":"South Carolina",
    "46":"South Dakota","47":"Tennessee","48":"Texas","49":"Utah","50":"Vermont",
    "51":"Virginia","53":"Washington","54":"West Virginia","55":"Wisconsin",
    "56":"Wyoming",
}

# Hardcoded CBSA lookup for the 50 largest metros (avoids API call for common queries).
# CBSA code → (BLS area code, metro name)
CBSA_TO_BLS = {
    "35620": ("M3562000", "New York-Newark-Jersey City, NY-NJ-PA"),
    "31080": ("M3108000", "Los Angeles-Long Beach-Anaheim, CA"),
    "16980": ("M1698000", "Chicago-Naperville-Elgin, IL-IN-WI"),
    "19100": ("M1910000", "Dallas-Fort Worth-Arlington, TX"),
    "26420": ("M2642000", "Houston-The Woodlands-Sugar Land, TX"),
    "33100": ("M3310000", "Miami-Fort Lauderdale-West Palm Beach, FL"),
    "47900": ("M4790000", "Washington-Arlington-Alexandria, DC-VA-MD-WV"),
    "37980": ("M3798000", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD"),
    "12060": ("M1206000", "Atlanta-Sandy Springs-Alpharetta, GA"),
    "14460": ("M1446000", "Boston-Cambridge-Newton, MA-NH"),
    "33460": ("M3346000", "Minneapolis-St. Paul-Bloomington, MN-WI"),
    "41860": ("M4186000", "San Francisco-Oakland-Berkeley, CA"),
    "41740": ("M4174000", "San Diego-Chula Vista-Carlsbad, CA"),
    "45300": ("M4530000", "Tampa-St. Petersburg-Clearwater, FL"),
    "19820": ("M1982000", "Detroit-Warren-Dearborn, MI"),
    "36740": ("M3674000", "Orlando-Kissimmee-Sanford, FL"),
    "12420": ("M1242000", "Austin-Round Rock-Georgetown, TX"),
    "38060": ("M3806000", "Phoenix-Mesa-Chandler, AZ"),
    "41700": ("M4170000", "San Antonio-New Braunfels, TX"),
    "29820": ("M2982000", "Las Vegas-Henderson-Paradise, NV"),
    "40140": ("M4014000", "Riverside-San Bernardino-Ontario, CA"),
    "17460": ("M1746000", "Cincinnati, OH-KY-IN"),
    "28140": ("M2814000", "Kansas City, MO-KS"),
    "39300": ("M3930000", "Portland-Vancouver-Hillsboro, OR-WA"),
    "41180": ("M4118000", "St. Louis, MO-IL"),
    "32580": ("M3258000", "McAllen-Edinburg-Mission, TX"),
    "12580": ("M1258000", "Baltimore-Columbia-Towson, MD"),
    "36420": ("M3642000", "Oklahoma City, OK"),
    "27260": ("M2726000", "Jacksonville, FL"),
    "46140": ("M4614000", "Tucson, AZ"),
    "42660": ("M4266000", "Seattle-Tacoma-Bellevue, WA"),
    "16740": ("M1674000", "Charlotte-Concord-Gastonia, NC-SC"),
    "24340": ("M2434000", "Grand Rapids-Kentwood, MI"),
    "10740": ("M1074000", "Albuquerque, NM"),
    "17140": ("M1714000", "Cleveland-Elyria, OH"),
    "18140": ("M1814000", "Columbus, OH"),
    "19740": ("M1974000", "Denver-Aurora-Lakewood, CO"),
    "30460": ("M3046000", "Louisville/Jefferson County, KY-IN"),
    "24660": ("M2466000", "Greenville-Anderson, SC"),
    "32820": ("M3282000", "Memphis, TN-MS-AR"),
    "33260": ("M3326000", "Milwaukee-Waukesha, WI"),
    "26900": ("M2690000", "Indianapolis-Carmel-Anderson, IN"),
    "16580": ("M1658000", "Charleston-North Charleston, SC"),
    "47260": ("M4726000", "Virginia Beach-Norfolk-Newport News, VA-NC"),
    "25540": ("M2554000", "Hartford-East Hartford-Middletown, CT"),
    "31460": ("M3146000", "Madison, WI"),
    "14260": ("M1426000", "Boise City, ID"),
    "34980": ("M3498000", "Nashville-Davidson-Murfreesboro-Franklin, TN"),
    "35380": ("M3538000", "New Orleans-Metairie, LA"),
    "40900": ("M4090000", "Sacramento-Roseville-Folsom, CA"),
    "38900": ("M3890000", "Raleigh-Cary, NC"),
    "41940": ("M4194000", "San Jose-Sunnyvale-Santa Clara, CA"),
}

# City name → CBSA code (hardcoded for 50 largest cities; API used for others)
CITY_TO_CBSA = {
    ("new york", "ny"):         "35620",
    ("los angeles", "ca"):      "31080",
    ("chicago", "il"):          "16980",
    ("dallas", "tx"):           "19100",
    ("houston", "tx"):          "26420",
    ("miami", "fl"):            "33100",
    ("washington", "dc"):       "47900",
    ("philadelphia", "pa"):     "37980",
    ("atlanta", "ga"):          "12060",
    ("boston", "ma"):           "14460",
    ("minneapolis", "mn"):      "33460",
    ("san francisco", "ca"):    "41860",
    ("san diego", "ca"):        "41740",
    ("tampa", "fl"):            "45300",
    ("detroit", "mi"):          "19820",
    ("orlando", "fl"):          "36740",
    ("austin", "tx"):           "12420",
    ("phoenix", "az"):          "38060",
    ("san antonio", "tx"):      "41700",
    ("las vegas", "nv"):        "29820",
    ("riverside", "ca"):        "40140",
    ("portland", "or"):         "39300",
    ("st. louis", "mo"):        "41180",
    ("seattle", "wa"):          "42660",
    ("charlotte", "nc"):        "16740",
    ("columbus", "oh"):         "18140",
    ("denver", "co"):           "19740",
    ("indianapolis", "in"):     "26900",
    ("nashville", "tn"):        "34980",
    ("new orleans", "la"):      "35380",
    ("sacramento", "ca"):       "40900",
    ("raleigh", "nc"):          "38900",
    ("san jose", "ca"):         "41940",
    ("baltimore", "md"):        "12580",
    ("oklahoma city", "ok"):    "36420",
    ("jacksonville", "fl"):     "27260",
    ("tucson", "az"):           "46140",
    ("albuquerque", "nm"):      "10740",
    ("cleveland", "oh"):        "17140",
    ("memphis", "tn"):          "32820",
    ("milwaukee", "wi"):        "33260",
    ("louisville", "ky"):       "30460",
    ("madison", "wi"):          "31460",
    ("boise", "id"):            "14260",
    ("hartford", "ct"):         "25540",
    ("richmond", "va"):         "40060",
    ("virginia beach", "va"):   "47260",
    ("salt lake city", "ut"):   "41620",
    ("kansas city", "mo"):      "28140",
    ("cincinnati", "oh"):       "17460",
}


def _parse_location(location_str: str) -> tuple[str, str]:
    """Parse 'City, ST' → ('city_lower', 'state_abbr_upper')"""
    location_str = location_str.strip()
    parts = [p.strip() for p in location_str.rsplit(",", 1)]
    if len(parts) == 2:
        city  = parts[0].lower()
        state = parts[1].upper()[:2]
        return city, state
    return location_str.lower(), ""


@lru_cache(maxsize=256)
def resolve_msa(location_str: str) -> dict:
    """
    Resolve a location string to geo codes.

    Returns:
        {
          "msa_code":   str or None,   # BLS area code, e.g. "M1242000"
          "msa_name":   str or None,
          "cbsa_fips":  str or None,
          "state_code": str or None,   # BLS state code, e.g. "S4800000"
          "state_name": str or None,
          "state_abbr": str or None,
        }
    """
    city, state = _parse_location(location_str)

    state_fips = STATE_FIPS.get(state)
    state_code = f"S{state_fips}00000" if state_fips else None
    state_name = STATE_NAMES.get(state_fips, state)

    result = {
        "msa_code":   None,
        "msa_name":   None,
        "cbsa_fips":  None,
        "state_code": state_code,
        "state_name": state_name,
        "state_abbr": state,
    }

    # 1. Hardcoded fast lookup
    cbsa = CITY_TO_CBSA.get((city, state.lower()))
    if not cbsa and state:
        # Try just city
        for (c, s), code in CITY_TO_CBSA.items():
            if c == city:
                cbsa = code
                break

    if cbsa and cbsa in CBSA_TO_BLS:
        bls_code, metro_name = CBSA_TO_BLS[cbsa]
        result.update({
            "msa_code":  bls_code,
            "msa_name":  metro_name,
            "cbsa_fips": cbsa,
        })
        return result

    # 2. Census geocoding API fallback
    # (only called for cities not in hardcoded list)
    try:
        census_result = _census_geocode(city, state)
        if census_result:
            cbsa_fips = census_result.get("cbsa_fips")
            if cbsa_fips and cbsa_fips in CBSA_TO_BLS:
                bls_code, metro_name = CBSA_TO_BLS[cbsa_fips]
                result.update({
                    "msa_code":  bls_code,
                    "msa_name":  metro_name,
                    "cbsa_fips": cbsa_fips,
                })
    except Exception as e:
        print(f"[Geo] Census geocoding failed: {e}")

    return result


def _census_geocode(city: str, state: str) -> Optional[dict]:
    """
    Use Census geocoding API to get lat/lon, then reverse geocode to CBSA.
    Two-step: address → coordinates → geographies (CBSA).
    """
    # Step 1: address → coordinates
    params = {
        "street":     city,
        "state":      state,
        "benchmark":  "2020",
        "format":     "json",
    }
    resp = requests.get(CENSUS_GEOCODE_BASE, params=params, timeout=10)
    data = resp.json()

    matches = data.get("result", {}).get("addressMatches", [])
    if not matches:
        return None

    coords = matches[0]["coordinates"]
    lon, lat = coords["x"], coords["y"]

    # Step 2: coordinates → geographies including CBSA
    params2 = {
        "x":          lon,
        "y":          lat,
        "benchmark":  "2020",
        "vintage":    "2020",
        "layers":     "Metropolitan Statistical Areas",
        "format":     "json",
    }
    resp2 = requests.get(CENSUS_CBSA_BASE, params=params2, timeout=10)
    geo_data = resp2.json()

    msas = (
        geo_data.get("result", {})
                .get("geographies", {})
                .get("Metropolitan Statistical Areas", [])
    )
    if msas:
        return {"cbsa_fips": msas[0].get("GEOID", "")}

    return None
