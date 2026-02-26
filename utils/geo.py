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

# CBSA code → (BLS area code, metro name)
CBSA_TO_BLS = {
    # ── Largest metros ────────────────────────────────────────────────────────
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
    "17140": ("M1714000", "Cleveland-Elyria, OH"),
    "18140": ("M1814000", "Columbus, OH"),
    "19740": ("M1974000", "Denver-Aurora-Lakewood, CO"),
    "30460": ("M3046000", "Louisville/Jefferson County, KY-IN"),
    "32820": ("M3282000", "Memphis, TN-MS-AR"),
    "33260": ("M3326000", "Milwaukee-Waukesha, WI"),
    "26900": ("M2690000", "Indianapolis-Carmel-Anderson, IN"),
    "25540": ("M2554000", "Hartford-East Hartford-Middletown, CT"),
    "31460": ("M3146000", "Madison, WI"),
    "14260": ("M1426000", "Boise City, ID"),
    "34980": ("M3498000", "Nashville-Davidson-Murfreesboro-Franklin, TN"),
    "35380": ("M3538000", "New Orleans-Metairie, LA"),
    "40900": ("M4090000", "Sacramento-Roseville-Folsom, CA"),
    "41940": ("M4194000", "San Jose-Sunnyvale-Santa Clara, CA"),
    "24340": ("M2434000", "Grand Rapids-Kentwood, MI"),
    "10740": ("M1074000", "Albuquerque, NM"),

    # ── North Carolina ────────────────────────────────────────────────────────
    "16740": ("M1674000", "Charlotte-Concord-Gastonia, NC-SC"),
    "38900": ("M3890000", "Raleigh-Cary, NC"),
    "11700": ("M1170000", "Asheville, NC"),
    "20500": ("M2050000", "Durham-Chapel Hill, NC"),
    "24140": ("M2414000", "Greensboro-High Point, NC"),
    "49180": ("M4918000", "Winston-Salem, NC"),
    "48900": ("M4890000", "Wilmington, NC"),
    "22180": ("M2218000", "Fayetteville, NC"),
    "25860": ("M2586000", "Hickory-Lenoir-Morganton, NC"),
    "15500": ("M1550000", "Burlington, NC"),
    "40580": ("M4058000", "Rocky Mount, NC"),
    "39580": ("M3958000", "Roanoke Rapids, NC"),

    # ── Southeast ─────────────────────────────────────────────────────────────
    "24660": ("M2466000", "Greenville-Anderson, SC"),
    "16580": ("M1658000", "Charleston-North Charleston, SC"),
    "47260": ("M4726000", "Virginia Beach-Norfolk-Newport News, VA-NC"),
    "40060": ("M4006000", "Richmond, VA"),
    "13980": ("M1398000", "Charlottesville, VA"),
    "44420": ("M4442000", "Roanoke, VA"),
    "16860": ("M1686000", "Chattanooga, TN-GA"),
    "27740": ("M2774000", "Knoxville, TN"),
    "34100": ("M3410000", "Murfreesboro, TN"),
    "26300": ("M2630000", "Huntsville, AL"),
    "13820": ("M1382000", "Birmingham-Hoover, AL"),
    "33660": ("M3366000", "Mobile, AL"),
    "37860": ("M3786000", "Pensacola-Ferry Pass-Brent, FL"),
    "18880": ("M1888000", "Daytona Beach, FL"),
    "42680": ("M4268000", "Sebastian-Vero Beach, FL"),
    "38940": ("M3894000", "Cape Coral-Fort Myers, FL"),

    # ── Mid-Atlantic / Northeast ──────────────────────────────────────────────
    "35300": ("M3530000", "New Haven-Milford, CT"),
    "35980": ("M3598000", "Norwich-New London, CT"),
    "39300": ("M3930000", "Providence-Warwick, RI-MA"),
    "15764": ("M1576400", "Albany-Schenectady-Troy, NY"),
    "45060": ("M4506000", "Syracuse, NY"),
    "40380": ("M4038000", "Rochester, NY"),
    "15380": ("M1538000", "Buffalo-Cheektowaga, NY"),
    "10580": ("M1058000", "Albany, NY"),
    "35614": ("M3561400", "Nassau County-Suffolk County, NY"),
    "35084": ("M3508400", "Newark, NJ-PA"),

    # ── Midwest ───────────────────────────────────────────────────────────────
    "19380": ("M1938000", "Dayton-Kettering, OH"),
    "18020": ("M1802000", "Akron, OH"),
    "17460": ("M1746000", "Cincinnati, OH-KY-IN"),
    "45780": ("M4578000", "Toledo, OH"),
    "16620": ("M1662000", "Champaign-Urbana, IL"),
    "44100": ("M4410000", "Rockford, IL"),
    "16580": ("M1658000", "Springfield, IL"),
    "28100": ("M2810000", "Kalamazoo-Portage, MI"),
    "26090": ("M2609000", "Holland, MI"),
    "22420": ("M2242000", "Flint, MI"),
    "20994": ("M2099400", "Eau Claire, WI"),
    "29404": ("M2940400", "La Crosse-Onalaska, WI-MN"),
    "24580": ("M2458000", "Green Bay, WI"),
    "31900": ("M3190000", "Lincoln, NE"),
    "36540": ("M3654000", "Omaha-Council Bluffs, NE-IA"),
    "19780": ("M1978000", "Des Moines-West Des Moines, IA"),
    "26980": ("M2698000", "Iowa City, IA"),
    "33460": ("M3346000", "Minneapolis-St. Paul-Bloomington, MN-WI"),
    "20260": ("M2026000", "Duluth, MN-WI"),
    "22060": ("M2206000", "Fargo, ND-MN"),
    "13140": ("M1314000", "Bismarck, ND"),
    "43780": ("M4378000", "Sioux Falls, SD"),
    "39380": ("M3938000", "Rapid City, SD"),
    "27060": ("M2706000", "Jefferson City, MO"),
    "41140": ("M4114000", "Springfield, MO"),
    "27900": ("M2790000", "Joplin, MO"),
    "28620": ("M2862000", "Lawrence, KS"),
    "28100": ("M2810000", "Topeka, KS"),
    "48620": ("M4862000", "Wichita, KS"),

    # ── Southwest / Mountain ──────────────────────────────────────────────────
    "19740": ("M1974000", "Denver-Aurora-Lakewood, CO"),
    "14500": ("M1450000", "Boulder, CO"),
    "24300": ("M2430000", "Fort Collins, CO"),
    "22660": ("M2266000", "Colorado Springs, CO"),
    "42340": ("M4234000", "Santa Fe, NM"),
    "29740": ("M2974000", "Las Cruces, NM"),
    "41620": ("M4162000", "Salt Lake City, UT"),
    "36260": ("M3626000", "Ogden-Clearfield, UT"),
    "39340": ("M3934000", "Provo-Orem, UT"),
    "39900": ("M3990000", "St. George, UT"),
    "39220": ("M3922000", "Reno, NV"),
    "29460": ("M2946000", "Carson City, NV"),
    "30860": ("M3086000", "Lubbock, TX"),
    "41660": ("M4166000", "San Angelo, TX"),
    "19124": ("M1912400", "Midland, TX"),
    "36220": ("M3622000", "Odessa, TX"),
    "22100": ("M2210000", "El Paso, TX"),
    "37100": ("M3710000", "Corpus Christi, TX"),
    "26420": ("M2642000", "Beaumont-Port Arthur, TX"),
    "13060": ("M1306000", "Abilene, TX"),
    "18580": ("M1858000", "College Station-Bryan, TX"),
    "26620": ("M2662000", "Killeen-Temple, TX"),
    "45500": ("M4550000", "Texarkana, TX-AR"),

    # ── Pacific Northwest / West ───────────────────────────────────────────────
    "13460": ("M1346000", "Bellingham, WA"),
    "36500": ("M3650000", "Olympia-Lacey-Tumwater, WA"),
    "45104": ("M4510400", "Tacoma-Lakewood, WA"),
    "28420": ("M2842000", "Kennewick-Richland, WA"),
    "24260": ("M2426000", "Grants Pass, OR"),
    "18700": ("M1870000", "Corvallis, OR"),
    "21660": ("M2166000", "Eugene-Springfield, OR"),
    "38900": ("M3890000", "Salem, OR"),
    "13460": ("M1346000", "Bend, OR"),
    "25420": ("M2542000", "Medford, OR"),

    # ── California ────────────────────────────────────────────────────────────
    "31460": ("M3146000", "Fresno, CA"),
    "25260": ("M2526000", "Bakersfield, CA"),
    "33700": ("M3370000", "Modesto, CA"),
    "44700": ("M4470000", "Stockton, CA"),
    "46700": ("M4670000", "Visalia, CA"),
    "42220": ("M4222000", "Santa Barbara-Santa Maria-Goleta, CA"),
    "37100": ("M3710000", "Oxnard-Thousand Oaks-Ventura, CA"),
    "40900": ("M4090000", "Salinas, CA"),
    "41500": ("M4150000", "Santa Cruz-Watsonville, CA"),
    "42100": ("M4210000", "Santa Rosa-Petaluma, CA"),
    "34900": ("M3490000", "Napa, CA"),
    "32900": ("M3290000", "Merced, CA"),
    "23420": ("M2342000", "El Centro, CA"),
    "40140": ("M4014000", "San Bernardino, CA"),
}

# City name → CBSA code
CITY_TO_CBSA = {
    # ── Major metros ──────────────────────────────────────────────────────────
    ("new york", "ny"):             "35620",
    ("los angeles", "ca"):          "31080",
    ("chicago", "il"):              "16980",
    ("dallas", "tx"):               "19100",
    ("houston", "tx"):              "26420",
    ("miami", "fl"):                "33100",
    ("washington", "dc"):           "47900",
    ("philadelphia", "pa"):         "37980",
    ("atlanta", "ga"):              "12060",
    ("boston", "ma"):               "14460",
    ("minneapolis", "mn"):          "33460",
    ("san francisco", "ca"):        "41860",
    ("san diego", "ca"):            "41740",
    ("tampa", "fl"):                "45300",
    ("detroit", "mi"):              "19820",
    ("orlando", "fl"):              "36740",
    ("austin", "tx"):               "12420",
    ("phoenix", "az"):              "38060",
    ("san antonio", "tx"):          "41700",
    ("las vegas", "nv"):            "29820",
    ("riverside", "ca"):            "40140",
    ("portland", "or"):             "39300",
    ("st. louis", "mo"):            "41180",
    ("saint louis", "mo"):          "41180",
    ("seattle", "wa"):              "42660",
    ("columbus", "oh"):             "18140",
    ("denver", "co"):               "19740",
    ("indianapolis", "in"):         "26900",
    ("nashville", "tn"):            "34980",
    ("new orleans", "la"):          "35380",
    ("sacramento", "ca"):           "40900",
    ("san jose", "ca"):             "41940",
    ("baltimore", "md"):            "12580",
    ("oklahoma city", "ok"):        "36420",
    ("jacksonville", "fl"):         "27260",
    ("tucson", "az"):               "46140",
    ("albuquerque", "nm"):          "10740",
    ("cleveland", "oh"):            "17140",
    ("memphis", "tn"):              "32820",
    ("milwaukee", "wi"):            "33260",
    ("louisville", "ky"):           "30460",
    ("madison", "wi"):              "31460",
    ("boise", "id"):                "14260",
    ("hartford", "ct"):             "25540",
    ("richmond", "va"):             "40060",
    ("virginia beach", "va"):       "47260",
    ("norfolk", "va"):              "47260",
    ("salt lake city", "ut"):       "41620",
    ("kansas city", "mo"):          "28140",
    ("cincinnati", "oh"):           "17460",
    ("pittsburgh", "pa"):           "38300",
    ("st. petersburg", "fl"):       "45300",
    ("saint petersburg", "fl"):     "45300",

    # ── North Carolina ────────────────────────────────────────────────────────
    ("charlotte", "nc"):            "16740",
    ("raleigh", "nc"):              "38900",
    ("asheville", "nc"):            "11700",
    ("durham", "nc"):               "20500",
    ("chapel hill", "nc"):          "20500",
    ("greensboro", "nc"):           "24140",
    ("high point", "nc"):           "24140",
    ("winston-salem", "nc"):        "49180",
    ("winston salem", "nc"):        "49180",
    ("wilmington", "nc"):           "48900",
    ("fayetteville", "nc"):         "22180",
    ("hickory", "nc"):              "25860",
    ("burlington", "nc"):           "15500",
    ("rocky mount", "nc"):          "40580",
    ("cary", "nc"):                 "38900",
    ("concord", "nc"):              "16740",
    ("gastonia", "nc"):             "16740",
    ("apex", "nc"):                 "38900",
    ("wake forest", "nc"):          "38900",
    ("mooresville", "nc"):          "16740",
    ("huntersville", "nc"):         "16740",
    ("boone", "nc"):                "14380",

    # ── Southeast ─────────────────────────────────────────────────────────────
    ("greenville", "sc"):           "24660",
    ("columbia", "sc"):             "17900",
    ("charleston", "sc"):           "16580",
    ("myrtle beach", "sc"):         "34820",
    ("savannah", "ga"):             "42340",
    ("augusta", "ga"):              "12260",
    ("chattanooga", "tn"):          "16860",
    ("knoxville", "tn"):            "27740",
    ("huntsville", "al"):           "26300",
    ("birmingham", "al"):           "13820",
    ("mobile", "al"):               "33660",
    ("montgomery", "al"):           "33860",
    ("pensacola", "fl"):            "37860",
    ("daytona beach", "fl"):        "18880",
    ("cape coral", "fl"):           "38940",
    ("fort myers", "fl"):           "38940",
    ("charlottesville", "va"):      "13980",
    ("roanoke", "va"):              "44420",
    ("lynchburg", "va"):            "31340",

    # ── Texas ─────────────────────────────────────────────────────────────────
    ("el paso", "tx"):              "22100",
    ("corpus christi", "tx"):       "37100",
    ("lubbock", "tx"):              "30860",
    ("midland", "tx"):              "19124",
    ("odessa", "tx"):               "36220",
    ("amarillo", "tx"):             "11100",
    ("waco", "tx"):                 "47380",
    ("killeen", "tx"):              "26620",
    ("college station", "tx"):      "18580",
    ("abilene", "tx"):              "13060",
    ("beaumont", "tx"):             "13140",

    # ── Mountain / Southwest ──────────────────────────────────────────────────
    ("boulder", "co"):              "14500",
    ("fort collins", "co"):         "24300",
    ("colorado springs", "co"):     "22660",
    ("pueblo", "co"):               "39380",
    ("santa fe", "nm"):             "42340",
    ("las cruces", "nm"):           "29740",
    ("ogden", "ut"):                "36260",
    ("provo", "ut"):                "39340",
    ("st. george", "ut"):           "39900",
    ("saint george", "ut"):         "39900",
    ("reno", "nv"):                 "39220",
    ("carson city", "nv"):          "29460",

    # ── Pacific Northwest ─────────────────────────────────────────────────────
    ("bellingham", "wa"):           "13460",
    ("olympia", "wa"):              "36500",
    ("spokane", "wa"):              "44060",
    ("tacoma", "wa"):               "45104",
    ("kennewick", "wa"):            "28420",
    ("eugene", "or"):               "21660",
    ("salem", "or"):                "41420",
    ("bend", "or"):                 "13460",
    ("medford", "or"):              "25420",
    ("corvallis", "or"):            "18700",

    # ── California ────────────────────────────────────────────────────────────
    ("fresno", "ca"):               "23420",
    ("bakersfield", "ca"):          "12540",
    ("stockton", "ca"):             "44700",
    ("modesto", "ca"):              "33700",
    ("santa barbara", "ca"):        "42220",
    ("santa rosa", "ca"):           "42100",
    ("oxnard", "ca"):               "37100",
    ("salinas", "ca"):              "41500",
    ("visalia", "ca"):              "46700",
    ("napa", "ca"):                 "34900",
    ("merced", "ca"):               "32900",

    # ── Midwest ───────────────────────────────────────────────────────────────
    ("dayton", "oh"):               "19380",
    ("akron", "oh"):                "18020",
    ("toledo", "oh"):               "45780",
    ("grand rapids", "mi"):         "24340",
    ("kalamazoo", "mi"):            "28100",
    ("flint", "mi"):                "22420",
    ("lansing", "mi"):              "29620",
    ("omaha", "ne"):                "36540",
    ("lincoln", "ne"):              "31900",
    ("des moines", "ia"):           "19780",
    ("iowa city", "ia"):            "26980",
    ("fargo", "nd"):                "22060",
    ("bismarck", "nd"):             "13140",
    ("sioux falls", "sd"):          "43780",
    ("rapid city", "sd"):           "39380",
    ("green bay", "wi"):            "24580",
    ("springfield", "il"):          "44100",
    ("rockford", "il"):             "44100",
    ("wichita", "ks"):              "48620",
    ("topeka", "ks"):               "28100",
    ("springfield", "mo"):          "41140",
    ("joplin", "mo"):               "27900",

    # ── Northeast ─────────────────────────────────────────────────────────────
    ("buffalo", "ny"):              "15380",
    ("rochester", "ny"):            "40380",
    ("syracuse", "ny"):             "45060",
    ("albany", "ny"):               "10580",
    ("new haven", "ct"):            "35300",
    ("bridgeport", "ct"):           "14860",
    ("springfield", "ma"):          "44140",
    ("worcester", "ma"):            "49340",
    ("providence", "ri"):           "39300",
    ("manchester", "nh"):           "31700",
    ("portland", "me"):             "38860",
    ("burlington", "vt"):           "15540",
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
