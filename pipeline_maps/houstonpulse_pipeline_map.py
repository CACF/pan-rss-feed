"""
HoustonPulse Category Pipeline Map

List of scrapers registered for the HoustonPulse category.
To test specific scrapers, comment out unused scrapers in SCRAPERS below.
"""

from scrapers.houstonpulse.houstonpulse import HoustonPulseRSSPipeline

SCRAPERS = [
    HoustonPulseRSSPipeline,
]
