"""
Fashion Category Pipeline Map

List of scrapers registered for the Fashion category.
To test specific scrapers, comment out unused scrapers in SCRAPERS below.
"""

from scrapers.fashion.glammagazine import GlamFashionRSSPipeline
from scrapers.fashion.fashiontimesmagazine import FashionTimesRSSPipeline
from scrapers.fashion.arabnews import ArabNewsFashionRSSPipeline
from scrapers.fashion.bridesandyou import BridesAndYouFashionRSSPipeline
from scrapers.fashion.tribune_fashion import ExpressTribuneFashionRSSPipeline
from scrapers.fashion.divaonline import DivaFashionRSSPipeline
from scrapers.fashion.hmagpak import HMagFashionPipeline
from scrapers.fashion.wwd import WWDRSSPipeline
from scrapers.fashion.claire_fashion import MarieClaireFashionRSSPipeline
from scrapers.fashion.sunday_fashion import SundayFashionRSSPipeline
from scrapers.fashion.whowearwhat import WhoWhatWearFashionRSSPipeline

SCRAPERS = [
    GlamFashionRSSPipeline,
    FashionTimesRSSPipeline,
    ArabNewsFashionRSSPipeline,
    BridesAndYouFashionRSSPipeline,
    ExpressTribuneFashionRSSPipeline,
    DivaFashionRSSPipeline,
    HMagFashionPipeline,
    WWDRSSPipeline,
    MarieClaireFashionRSSPipeline,
    SundayFashionRSSPipeline,
    WhoWhatWearFashionRSSPipeline
]
