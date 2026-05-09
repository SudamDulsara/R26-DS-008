"""Input/output adapters. Loaders read upstream data, writers emit reports/corpus."""
from .loader import JsonlLoader
from .writer import XlsxWriter

__all__ = ["JsonlLoader", "XlsxWriter"]