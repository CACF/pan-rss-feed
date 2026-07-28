"""
MeraMurree Category Pipeline Map

List of scrapers registered for the MeraMurree category.
To test specific scrapers, comment out unused scrapers in SCRAPERS below.
"""

from scrapers.meramuree.meramuree import PotoharRSSPipeline

SCRAPERS = [
    PotoharRSSPipeline,
]
