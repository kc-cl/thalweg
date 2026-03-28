"""Data source fetchers."""

from thalweg.fetchers.boc import BoCFetcher
from thalweg.fetchers.boe import BoEFetcher
from thalweg.fetchers.ecb import ECBFetcher
from thalweg.fetchers.overnight import OvernightRatesFetcher
from thalweg.fetchers.ust import USTFetcher

FETCHERS = {
    "boc": BoCFetcher,
    "boe": BoEFetcher,
    "ecb": ECBFetcher,
    "ust": USTFetcher,
}

RATE_FETCHERS = {
    "overnight": OvernightRatesFetcher,
}

__all__ = [
    "FETCHERS",
    "RATE_FETCHERS",
    "BoCFetcher",
    "BoEFetcher",
    "ECBFetcher",
    "OvernightRatesFetcher",
    "USTFetcher",
]
