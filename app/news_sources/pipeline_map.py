import logging
from app.news_sources.rss_sources.murree import MurreeRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    "Murree": MurreeRSSPipeline,
}
