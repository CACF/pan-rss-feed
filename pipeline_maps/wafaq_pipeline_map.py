"""
Wafaq Category Pipeline Map

List of scrapers registered for the Wafaq category.
To test specific scrapers, comment out unused scrapers in SCRAPERS below.
"""

from scrapers.wafaq.wafaq import IslamabadRSSPipeline

SCRAPERS = [
    IslamabadRSSPipeline,
]
