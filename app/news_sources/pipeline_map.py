import logging
from app.news_sources.rss_sources.ary import ARYNewsBusinessRSSPipeline
from app.news_sources.rss_sources.bb import BloombergRSSPipeline
from app.news_sources.rss_sources.bbc import BBCRSSPipeline
from app.news_sources.rss_sources.br import BusinessRecorderRSSPipeline
from app.news_sources.rss_sources.cnbc import CNBCRSSPipeline
from app.news_sources.rss_sources.dn import DawnRSSPipeline
from app.news_sources.rss_sources.dp import DailyPakistanBusinessRSSPipeline
from app.news_sources.rss_sources.geo import GeoNewsBusinessRSSPipeline
from app.news_sources.rss_sources.pbc import PBCNewsRSSPipeline
from app.news_sources.rss_sources.pp import ProfitPakistanTodayRSSPipeline
from app.news_sources.rss_sources.trb import TribuneRSSPipeline
from app.news_sources.rss_sources.WSJ import WSJRSSPipeline
from app.news_sources.scraper_sources.mttg import MettisglobalBusinessScraper
from app.news_sources.scraper_sources.tfd import FinancialDailyBusinessPipeline
from app.news_sources.rss_sources.exp_pk import ExpressUrduBusinessRSSPipeline
from app.news_sources.rss_sources.aaj import AajTVBusinessRSSPipeline
from app.news_sources.rss_sources.bol import BOLNewsBusinessRSSPipeline
from app.news_sources.rss_sources.tc import TradeChronicleRSSPipeline
from app.news_sources.rss_sources.propak import ProPakistaniBusinessRSSPipeline
from app.news_sources.rss_sources.cd import CoinDeskRSSPipeline
from app.news_sources.rss_sources.decrypt import DecryptRSSPipeline
from app.news_sources.rss_sources.bitcoin_magzine import BitcoinMagazineRSSPipeline
from app.news_sources.rss_sources.blockworks import BlockworksRSSPipeline
from app.news_sources.rss_sources.AiNews import AIBusinessStrategyRSSPipeline
from app.news_sources.rss_sources.MIT import MITTechnologyReviewAIRSSPipeline
from app.news_sources.rss_sources.Techcrunch import TechCrunchRSSPipeline
from app.news_sources.rss_sources.VentureBeat import VentureBeatBusinessRSSPipeline
from app.news_sources.rss_sources.wired import WiredBusinessRSSPipeline
from app.news_sources.rss_sources.theverge import TheVergeRSSPipeline
from app.news_sources.rss_sources.theregister import TheRegisterRSSPipeline
from app.news_sources.rss_sources.forbes import ForbesRSSPipeline
from app.news_sources.rss_sources.phoneworld import PhoneWorldRSSPipeline
from app.news_sources.rss_sources.techx import TechXRSSPipeline
from app.news_sources.rss_sources.pakrealestatetimes import (
    PakistanRealEstateTimesRSSPipeline,
)
from app.news_sources.rss_sources.plotistaan import PlotistanRSSPipeline
from app.news_sources.rss_sources.zameen_com import ZameenRSSPipeline
from app.news_sources.rss_sources.graana_com import GraanaRSSPipeline
from app.news_sources.rss_sources.APP import APPBusinessRSSPipeline
from app.news_sources.rss_sources.pakbiz import PakbizRSSPipeline
from app.news_sources.rss_sources.Asports import ASportsFootballRSSPipeline
from app.news_sources.rss_sources.Tensports import TenSportsRSSPipeline
from app.news_sources.rss_sources.espn import ESPNScraper
from app.news_sources.rss_sources.GuardianFootball import GuardianFootballRSSPipeline
from app.news_sources.rss_sources.YahooSports import YahooSportsRSSPipeline
from app.news_sources.rss_sources.cbsSports import CBSSportsScraper
from app.news_sources.rss_sources.goal import GoalSportsNewsPipeline
from app.news_sources.rss_sources.nytimes import NYTSoccerRSSPipeline
from app.news_sources.rss_sources.foxsports import FoxSportsRSSPipeline
from app.news_sources.rss_sources.eyefootball import EyefootballRSSPipeline
from app.news_sources.rss_sources.bostonglobe import BostonGlobeSportsRSSPipeline
from app.news_sources.rss_sources.glammagazine import GlamFashionRSSPipeline
from app.news_sources.rss_sources.fashiontimesmagazine import FashionTimesRSSPipeline
from app.news_sources.rss_sources.arabnews import ArabNewsFashionRSSPipeline
from app.news_sources.rss_sources.bridesandyou import BridesAndYouFashionRSSPipeline
from app.news_sources.rss_sources.tribune_fashion import (
    ExpressTribuneFashionRSSPipeline,
)
from app.news_sources.rss_sources.divaonline import DivaFashionRSSPipeline
from app.news_sources.rss_sources.hmagpak import HMagFashionPipeline
from app.news_sources.rss_sources.wwd import WWDRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    # "Reuters": ReutersRSSPipeline,
    "aaj": AajTVBusinessRSSPipeline,
    "APP NEWS": APPBusinessRSSPipeline,
    "BBC": BBCRSSPipeline,
    "CNBC": CNBCRSSPipeline,
    "mttg": MettisglobalBusinessScraper,
    "trb": TribuneRSSPipeline,
    "br": BusinessRecorderRSSPipeline,
    "dn": DawnRSSPipeline,
    "dp": DailyPakistanBusinessRSSPipeline,
    "ary": ARYNewsBusinessRSSPipeline,
    "geo": GeoNewsBusinessRSSPipeline,
    "exp_pk": ExpressUrduBusinessRSSPipeline,
    "bol": BOLNewsBusinessRSSPipeline,
    "tc": TradeChronicleRSSPipeline,
    "cd": CoinDeskRSSPipeline,
    "propak": ProPakistaniBusinessRSSPipeline,
    "decrypt": DecryptRSSPipeline,
    "bitcoin_magzine": BitcoinMagazineRSSPipeline,
    "AiNews": AIBusinessStrategyRSSPipeline,
    "MIT": MITTechnologyReviewAIRSSPipeline,
    "Techcrunch": TechCrunchRSSPipeline,
    "wired": WiredBusinessRSSPipeline,
    "theverge": TheVergeRSSPipeline,
    "theregister": TheRegisterRSSPipeline,
    "zameen_com": ZameenRSSPipeline,
    "graana_com": GraanaRSSPipeline,
    "pakbiz": PakbizRSSPipeline,
    "techx": TechXRSSPipeline,
    "Asports": ASportsFootballRSSPipeline,
    "Tensports": TenSportsRSSPipeline,
    "espn": ESPNScraper,
    "GuardianFootball": GuardianFootballRSSPipeline,
    "yahosports": YahooSportsRSSPipeline,
    "cbsSports": CBSSportsScraper,
    "goal": GoalSportsNewsPipeline,
    "nytimes": NYTSoccerRSSPipeline,
    "foxsports": FoxSportsRSSPipeline,
    "eyefootball": EyefootballRSSPipeline,
    "bostonglobe": BostonGlobeSportsRSSPipeline,
    "glammagazine": GlamFashionRSSPipeline,  # Fashion
    "fashiontimesmagazine": FashionTimesRSSPipeline,  # Fashion
    "arabnews": ArabNewsFashionRSSPipeline,
    "bridesandyou": BridesAndYouFashionRSSPipeline,
    "tribune": ExpressTribuneFashionRSSPipeline,
    "divaonline": DivaFashionRSSPipeline,
    "hmagpak": HMagFashionPipeline,
    "wwd": WWDRSSPipeline,
}
