import logging
import os
from dotenv import load_dotenv

from app.news_sources.pipeline_map import PIPELINE_MAP
from app.tasks.news_scraper_tasks import run_pipelines_concurrently

load_dotenv()

# --- Setup logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    Run news scraping pipelines concurrently.
    Edit the variables below if needed.
    """

    sources = list(PIPELINE_MAP.keys())
    input_data = {
        # "email": "user@example.com"
    }
    timeout_seconds = None

    if not sources:
        logger.error("No valid sources provided")
        return

    logger.info(f"Starting concurrent scraping for sources: {sources}")

    results = run_pipelines_concurrently(
        sources=sources,
        input_data=input_data,
        timeout_seconds=timeout_seconds,
    )

    logger.info("Scraping completed")
    logger.info(f"Results: {results}")


if __name__ == "__main__":
    main()
