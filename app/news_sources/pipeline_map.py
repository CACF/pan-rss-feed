import logging
from app.news_sources.rss_sources.islamabad import IslamabadRSSPipeline

logger = logging.getLogger(__name__)

PIPELINE_MAP = {
    "islamabad": IslamabadRSSPipeline,
}
