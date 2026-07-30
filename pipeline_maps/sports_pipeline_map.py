"""
Sports Category Pipeline Map

List of scrapers registered for the Sports category.
To test specific scrapers, comment out unused scrapers in SCRAPERS below.
"""

from scrapers.sports.Asports import ASportsFootballRSSPipeline
from scrapers.sports.GuardianFootball import GuardianFootballRSSPipeline
from scrapers.sports.PGAtour import PGATourRSSPipeline
from scrapers.sports.Tensports import TenSportsRSSPipeline
from scrapers.sports.YahooSports import YahooSportsRSSPipeline
from scrapers.sports.bostonglobe import BostonGlobeSportsRSSPipeline
from scrapers.sports.cbsSports import CBSSportsScraper
from scrapers.sports.espn import ESPNScraper
from scrapers.sports.eyefootball import EyefootballRSSPipeline
from scrapers.sports.four_four_two import FourFourTwoRSSPipeline
from scrapers.sports.foxsports import FoxSportsRSSPipeline
from scrapers.sports.goal import GoalSportsNewsPipeline
from scrapers.sports.nytimes import NYTSoccerRSSPipeline
from scrapers.sports.world_soccer import WorldSoccerRSSPipeline
from scrapers.sports.CBCSports import CBCSportsScraper
from scrapers.sports.FIHHockey import FIHHockeyScraper
from scrapers.sports.HockeyPaper import HockeyPaperScraper
from scrapers.sports.geoSuper import GeosuperScraper
from scrapers.sports.sportsNet import SportsnetScraper
from scrapers.sports.dp_sports import DailyPakistanSportsRSSPipeline
from scrapers.sports.app_sports import APPSportsRSSPipeline
from scrapers.sports.bol_sports import BOLNewsSportsRSSPipeline
from scrapers.sports.trb_sports import TribuneSportsRSSPipeline
from scrapers.sports.tc_sports import TradeChronicleSportsRSSPipeline

SCRAPERS = [
    ASportsFootballRSSPipeline,
    GuardianFootballRSSPipeline,
    PGATourRSSPipeline,
    TenSportsRSSPipeline,
    YahooSportsRSSPipeline,
    BostonGlobeSportsRSSPipeline,
    CBSSportsScraper,
    ESPNScraper,
    EyefootballRSSPipeline,
    FourFourTwoRSSPipeline,
    FoxSportsRSSPipeline,
    GoalSportsNewsPipeline,
    NYTSoccerRSSPipeline,
    WorldSoccerRSSPipeline,
    CBCSportsScraper,
    FIHHockeyScraper,
    HockeyPaperScraper,
    GeosuperScraper,
    SportsnetScraper,
    DailyPakistanSportsRSSPipeline,
    APPSportsRSSPipeline,
    BOLNewsSportsRSSPipeline,
    TribuneSportsRSSPipeline,
    TradeChronicleSportsRSSPipeline,
]
