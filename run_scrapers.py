import logging
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

from app.news_sources.pipeline_map import PIPELINE_MAP
from app.tasks.news_scraper_tasks import run_pipelines_concurrently

load_dotenv()

# --- Setup logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

user = quote_plus(os.getenv("DB_USER", ""))
password = quote_plus(os.getenv("DB_PW", ""))
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "27017")
db_name = os.getenv("DB_NAME", "Karobaar")

mongo_connection_string = f"mongodb://{user}:{password}@{host}:{port}/"

# Optional: set an environment variable so your utilities use it
os.environ["MONGO_CONNECTION_STRING"] = mongo_connection_string


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
