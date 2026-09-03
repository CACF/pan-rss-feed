"""
Business Category Pipeline Map

List of scrapers registered for the Business category.
To test specific scrapers, comment out unused scrapers in SCRAPERS below.
"""

from scrapers.business.ary import ARYNewsBusinessRSSPipeline
from scrapers.business.bb import BloombergRSSPipeline
from scrapers.business.bbc import BBCRSSPipeline
from scrapers.business.br import BusinessRecorderRSSPipeline
from scrapers.business.cnbc import CNBCRSSPipeline
from scrapers.business.dn import DawnRSSPipeline
from scrapers.business.dp import DailyPakistanBusinessRSSPipeline
from scrapers.business.geo import GeoNewsBusinessRSSPipeline
from scrapers.business.pbc import PBCNewsRSSPipeline
from scrapers.business.pp import ProfitPakistanTodayRSSPipeline
from scrapers.business.trb import TribuneRSSPipeline
from scrapers.business.WSJ import WSJRSSPipeline
from scrapers.business.mttg import MettisglobalBusinessScraper
from scrapers.business.tfd import FinancialDailyBusinessPipeline
from scrapers.business.exp_pk import ExpressUrduBusinessRSSPipeline
from scrapers.business.aaj import AajTVBusinessRSSPipeline
from scrapers.business.bol import BOLNewsBusinessRSSPipeline
from scrapers.business.tc import TradeChronicleRSSPipeline
from scrapers.business.propak import ProPakistaniBusinessRSSPipeline
from scrapers.business.cd import CoinDeskRSSPipeline
from scrapers.business.decrypt import DecryptRSSPipeline
from scrapers.business.bitcoin_magzine import BitcoinMagazineRSSPipeline
from scrapers.business.blockworks import BlockworksRSSPipeline
from scrapers.business.AiNews import AIBusinessStrategyRSSPipeline
from scrapers.business.MIT import MITTechnologyReviewAIRSSPipeline
from scrapers.business.Techcrunch import TechCrunchRSSPipeline
from scrapers.business.VentureBeat import VentureBeatBusinessRSSPipeline
from scrapers.business.wired import WiredBusinessRSSPipeline
from scrapers.business.theverge import TheVergeRSSPipeline
from scrapers.business.theregister import TheRegisterRSSPipeline
from scrapers.business.forbes import ForbesRSSPipeline
from scrapers.business.phoneworld import PhoneWorldRSSPipeline
from scrapers.business.techx import TechXRSSPipeline
from scrapers.business.pakrealestatetimes import PakistanRealEstateTimesRSSPipeline
from scrapers.business.plotistaan import PlotistanRSSPipeline
from scrapers.business.zameen_com import ZameenRSSPipeline
from scrapers.business.graana_com import GraanaRSSPipeline
from scrapers.business.APP import APPBusinessRSSPipeline
from scrapers.business.pakbiz import PakbizRSSPipeline
from scrapers.business.theblock import TheBlockRSSPipeline

SCRAPERS = [
    ARYNewsBusinessRSSPipeline,
    BloombergRSSPipeline,
    BBCRSSPipeline,
    # BusinessRecorderRSSPipeline,
    CNBCRSSPipeline,
    DawnRSSPipeline,
    DailyPakistanBusinessRSSPipeline,
    GeoNewsBusinessRSSPipeline,
    PBCNewsRSSPipeline,
    ProfitPakistanTodayRSSPipeline,
    TribuneRSSPipeline,
    WSJRSSPipeline,
    MettisglobalBusinessScraper,
    FinancialDailyBusinessPipeline,
    ExpressUrduBusinessRSSPipeline,
    AajTVBusinessRSSPipeline,
    BOLNewsBusinessRSSPipeline,
    TradeChronicleRSSPipeline,
    ProPakistaniBusinessRSSPipeline,
    CoinDeskRSSPipeline,
    DecryptRSSPipeline,
    BitcoinMagazineRSSPipeline,
    BlockworksRSSPipeline,
    AIBusinessStrategyRSSPipeline,
    MITTechnologyReviewAIRSSPipeline,
    TechCrunchRSSPipeline,
    VentureBeatBusinessRSSPipeline,
    WiredBusinessRSSPipeline,
    TheVergeRSSPipeline,
    TheRegisterRSSPipeline,
    ForbesRSSPipeline,
    PhoneWorldRSSPipeline,
    TechXRSSPipeline,
    PakistanRealEstateTimesRSSPipeline,
    PlotistanRSSPipeline,
    ZameenRSSPipeline,
    GraanaRSSPipeline,
    APPBusinessRSSPipeline,
    PakbizRSSPipeline,
    TheBlockRSSPipeline,
]
