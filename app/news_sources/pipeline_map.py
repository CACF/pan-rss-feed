import logging
from app.news_sources.rss_sources.hp import HoustonPulseRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    "Houston-Pulse": HoustonPulseRSSPipeline,
}
