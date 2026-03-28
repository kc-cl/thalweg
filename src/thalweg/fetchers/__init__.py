"""Data source fetchers."""

from thalweg.fetchers.boc import BoCFetcher
from thalweg.fetchers.ust import USTFetcher

FETCHERS = {
    "boc": BoCFetcher,
    "ust": USTFetcher,
}

__all__ = ["FETCHERS", "BoCFetcher", "USTFetcher"]
