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
from app.news_sources.rss_sources.pakrealestatetimes import PakistanRealEstateTimesRSSPipeline
from app.news_sources.rss_sources.plotistaan import PlotistanRSSPipeline
from app.news_sources.rss_sources.zameen_com import ZameenRSSPipeline
from app.news_sources.rss_sources.graana_com import GraanaRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    # "Reuters": ReutersRSSPipeline,
    # "APP NEWS": APPNewsPipeline,
    "BBC": BBCRSSPipeline,
    "PBC": PBCNewsRSSPipeline,
    "TFD": FinancialDailyBusinessPipeline,
    "CNBC" : CNBCRSSPipeline,
    "mttg" : MettisglobalBusinessScraper,
    "trb" : TribuneRSSPipeline,
    "br" : BusinessRecorderRSSPipeline,
    "dn" : DawnRSSPipeline,
    "dp" : DailyPakistanBusinessRSSPipeline,
    "ary" : ARYNewsBusinessRSSPipeline,
    "geo" : GeoNewsBusinessRSSPipeline,
    "exp_pk" : ExpressUrduBusinessRSSPipeline,
    "aaj" : AajTVBusinessRSSPipeline,
    "bol" : BOLNewsBusinessRSSPipeline,
    "tc" : TradeChronicleRSSPipeline,
    "cd" : CoinDeskRSSPipeline,
    "propak" : ProPakistaniBusinessRSSPipeline,
    "decrypt" : DecryptRSSPipeline,
    "bitcoin_magzine" : BitcoinMagazineRSSPipeline,
    "blockworks" : BlockworksRSSPipeline,
    "AiNews" : AIBusinessStrategyRSSPipeline,
    "MIT" : MITTechnologyReviewAIRSSPipeline,
    "Techcrunch" : TechCrunchRSSPipeline,
    "VentureBeat" : VentureBeatBusinessRSSPipeline,
    "wired" : WiredBusinessRSSPipeline,
    "theverge" : TheVergeRSSPipeline,
    "theregister" : TheRegisterRSSPipeline,
    "phoneworld" : PhoneWorldRSSPipeline,
    "plotistaan" : PlotistanRSSPipeline,
    "zameen_com" : ZameenRSSPipeline,
    "graana_com" : GraanaRSSPipeline
}
