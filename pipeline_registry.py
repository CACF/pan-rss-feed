"""
Central Pipeline Registry

Register category pipelines here.
To test specific categories, comment out unused pipelines.
"""

from scrapers.business.pipeline import BusinessPipeline
from scrapers.sports.pipeline import SportsPipeline
from scrapers.fashion.pipeline import FashionPipeline
from scrapers.entertainment.pipeline import EntertainmentPipeline
from scrapers.wafaq.pipeline import WafaqPipeline

PIPELINES = [
    # BusinessPipeline,
    # SportsPipeline,
    # FashionPipeline,
    # EntertainmentPipeline,
    WafaqPipeline,
]
