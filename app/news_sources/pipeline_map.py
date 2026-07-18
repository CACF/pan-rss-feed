import logging
from app.news_sources.rss_sources.potohar import PotoharRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    "Potohar": PotoharRSSPipeline,
}
