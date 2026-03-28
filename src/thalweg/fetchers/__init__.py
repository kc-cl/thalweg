"""Data source fetchers."""

from thalweg.fetchers.boc import BoCFetcher
from thalweg.fetchers.ecb import ECBFetcher
from thalweg.fetchers.ust import USTFetcher

FETCHERS = {
    "boc": BoCFetcher,
    "ecb": ECBFetcher,
    "ust": USTFetcher,
}

__all__ = ["FETCHERS", "BoCFetcher", "ECBFetcher", "USTFetcher"]
