from scrapers.karachi.dn import DawnRSSPipeline
from scrapers.karachi.tribune import TribuneKarachiRSSPipeline
from scrapers.karachi.karachi_alerts import KarachiAlertsRSSPipeline
from scrapers.karachi.times_of_karachi import TimesOfKarachiRSSPipeline
from scrapers.karachi.karachi_observer import KarachiObserverRSSPipeline
from scrapers.karachi.geo_karachi import GeoNewsKarachiRSSPipeline
from scrapers.karachi.ary_karachi import ARYNewsKarachiRSSPipeline
from scrapers.karachi.business_rec_karachi import BusinessRecorderKarachiRSSPipeline
from scrapers.karachi.jang_karachi import JangNewsKarachiRSSPipeline
from scrapers.karachi.exp_karachi import ExpressUrduKarachiRSSPipeline
from scrapers.karachi.sama_karachi import SamaaNewsKarachiPipeline
from scrapers.karachi.dunya_karachi import DunyaNewsKarachiPipeline
from scrapers.karachi.commisioner_karachi import CommissionerKarachiPipeline
from scrapers.karachi.kmc import KMCPipeline

SCRAPERS = [
    DawnRSSPipeline,
    TribuneKarachiRSSPipeline,
    KarachiAlertsRSSPipeline,
    TimesOfKarachiRSSPipeline,
    KarachiObserverRSSPipeline,
    GeoNewsKarachiRSSPipeline,
    ARYNewsKarachiRSSPipeline,
    BusinessRecorderKarachiRSSPipeline,
    JangNewsKarachiRSSPipeline,
    ExpressUrduKarachiRSSPipeline,
    SamaaNewsKarachiPipeline,
    DunyaNewsKarachiPipeline,
    CommissionerKarachiPipeline,
    KMCPipeline
]