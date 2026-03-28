"""Centralized configuration for Thalweg.

Reads settings from environment variables with sensible defaults.
"""

import os
from pathlib import Path

# --- Data directories ---

DATA_DIR = Path(os.environ.get("THALWEG_DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"
CURVES_DIR = DATA_DIR / "curves"
RATES_DIR = DATA_DIR / "rates"
DERIVED_DIR = DATA_DIR / "derived"

# Ensure directories exist
for _dir in (RAW_DIR, CURVES_DIR, RATES_DIR, DERIVED_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# --- API base URLs ---

BOC_BASE_URL = "https://www.bankofcanada.ca/valet"
UST_BASE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
)
ECB_BASE_URL = "https://data-api.ecb.europa.eu/service/data"
BOE_BASE_URL = "https://www.bankofengland.co.uk/statistics/yield-curves"
FED_H15_BASE_URL = "https://www.federalreserve.gov/datadownload"
