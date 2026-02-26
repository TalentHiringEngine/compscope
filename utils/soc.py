"""
SOC Mapper
==========
Utility to validate and normalize SOC codes, convert between
SOC-2010, SOC-2018, and SOC-2022 systems, and look up
related occupations for broadening a search when data is sparse.

BLS OEWS currently uses SOC-2018 for most series.
O*NET uses SOC-2019 (aligned with SOC-2018 with minor additions).
"""

import re
from typing import Optional


# SOC broad group → major group label
SOC_MAJOR_GROUPS = {
    "11": "Management",
    "13": "Business & Financial Operations",
    "15": "Computer & Mathematical",
    "17": "Architecture & Engineering",
    "19": "Life, Physical & Social Science",
    "21": "Community & Social Service",
    "23": "Legal",
    "25": "Educational Instruction",
    "27": "Arts, Design, Entertainment, Sports, Media",
    "29": "Healthcare Practitioners & Technical",
    "31": "Healthcare Support",
    "33": "Protective Service",
    "35": "Food Preparation & Serving",
    "37": "Building & Grounds Cleaning",
    "39": "Personal Care & Service",
    "41": "Sales & Related",
    "43": "Office & Administrative Support",
    "45": "Farming, Fishing & Forestry",
    "47": "Construction & Extraction",
    "49": "Installation, Maintenance & Repair",
    "51": "Production",
    "53": "Transportation & Material Moving",
}

# When detailed SOC data is unavailable in a metro, fall back to these
# broader aggregate codes for the same major group.
BROADER_FALLBACK = {
    "15-1252.00": ["15-1250",   "15-1200",   "15-0000"],   # SWE → Developers → Comp Occ → All CS
    "15-2051.00": ["15-2050",   "15-1200",   "15-0000"],   # Data Sci
    "15-1243.00": ["15-1240",   "15-1200",   "15-0000"],   # DB Architect
    "15-1244.00": ["15-1240",   "15-1200",   "15-0000"],   # Sysadmin
    "15-1212.00": ["15-1210",   "15-1200",   "15-0000"],   # InfoSec
    "29-1141.00": ["29-1140",   "29-1000",   "29-0000"],   # RN
    "13-2051.00": ["13-2000",   "13-0000",   None],        # Financial analyst
    "11-2021.00": ["11-2000",   "11-0000",   None],        # Mktg mgr
}


class SOCMapper:

    @staticmethod
    def clean(soc_code: str) -> str:
        """
        Normalize SOC code to XX-XXXX.XX format.
        Accepts: 151252, 15-1252, 15-1252.00, etc.
        """
        digits = re.sub(r"[^\d]", "", soc_code)
        if len(digits) == 6:
            return f"{digits[:2]}-{digits[2:6]}.00"
        if len(digits) == 8:
            return f"{digits[:2]}-{digits[2:6]}.{digits[6:8]}"
        return soc_code  # Return as-is if can't parse

    @staticmethod
    def for_bls_series(soc_code: str) -> str:
        """
        Convert SOC code to the 6-digit zero-padded format used
        in BLS OEWS series IDs (no hyphen, no decimal).
        e.g. "15-1252.00" → "151252"
        """
        return re.sub(r"[^\d]", "", soc_code)[:6]

    @staticmethod
    def major_group(soc_code: str) -> str:
        """Return the major group prefix (first 2 digits)."""
        return re.sub(r"[^\d]", "", soc_code)[:2]

    @staticmethod
    def describe(soc_code: str) -> str:
        """Return the major group label for a SOC code."""
        mg = SOCMapper.major_group(soc_code)
        return SOC_MAJOR_GROUPS.get(mg, "Unknown")

    @staticmethod
    def fallback_chain(soc_code: str) -> list[str]:
        """
        Return a list of progressively broader SOC codes to try
        when detailed occupation data is unavailable.

        e.g. "15-1252.00" → ["15-1250", "15-1200", "15-0000"]
        """
        clean = SOCMapper.clean(soc_code)

        # Check hardcoded map first
        if clean in BROADER_FALLBACK:
            return [c for c in BROADER_FALLBACK[clean] if c]

        # Generic fallback: strip to minor group, then major group, then all
        digits = re.sub(r"[^\d]", "", soc_code)
        mg     = digits[:2]
        minor  = digits[:4]
        return [
            f"{mg}-{minor}0",    # minor group (e.g. 15-1250)
            f"{mg}-{mg}00",      # major group (e.g. 15-1200) — approximate
            f"{mg}-0000",        # all in major (e.g. 15-0000)
        ]
