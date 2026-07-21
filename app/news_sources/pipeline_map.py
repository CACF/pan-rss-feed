import logging
from app.news_sources.rss_sources.houston import HoustonPulseRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    "Houston-Pulse": HoustonPulseRSSPipeline,
}
