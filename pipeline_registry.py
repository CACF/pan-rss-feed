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
from scrapers.meramuree.pipeline import MeraMurreePipeline
from scrapers.houstonpulse.pipeline import HoustonPulsePipeline

PIPELINES = [
    BusinessPipeline,
    SportsPipeline,
    FashionPipeline,
    WafaqPipeline,
    MeraMurreePipeline,
    HoustonPulsePipeline,
    # EntertainmentPipeline,
]
