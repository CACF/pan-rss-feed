from scrapers.karachi.dn import DawnRSSPipeline
from scrapers.karachi.tribune import TribuneKarachiRSSPipeline
from scrapers.karachi.karachi_alerts import KarachiAlertsRSSPipeline
from scrapers.karachi.times_of_karachi import TimesOfKarachiRSSPipeline
from scrapers.karachi.karachi_observer import KarachiObserverRSSPipeline

SCRAPERS = [
    DawnRSSPipeline,
    TribuneKarachiRSSPipeline,
    KarachiAlertsRSSPipeline,
    TimesOfKarachiRSSPipeline,
    KarachiObserverRSSPipeline
]