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

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    # "Reuters": ReutersRSSPipeline,
    # "APP NEWS": APPNewsPipeline,
    "BBC": BBCRSSPipeline,
    "WSJ": WSJRSSPipeline,
    "PBC": PBCNewsRSSPipeline,
    "TFD": FinancialDailyBusinessPipeline,
    "BB": BloombergRSSPipeline,
    "CNBC" : CNBCRSSPipeline,
    "mttg" : MettisglobalBusinessScraper,
    "trb" : TribuneRSSPipeline,
    "br" : BusinessRecorderRSSPipeline,
    "dn" : DawnRSSPipeline,
    "pp" : ProfitPakistanTodayRSSPipeline,
    "dp" : DailyPakistanBusinessRSSPipeline,
    "ary" : ARYNewsBusinessRSSPipeline,
    "geo" : GeoNewsBusinessRSSPipeline
    
}
