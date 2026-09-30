import logging
import requests
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from app.utilities import get_random_headers

logger = logging.getLogger(__name__)


def fetch_rss_feed(feed_url, process_item_func, max_workers=10, **kwargs):
    try:
        logger.info(f"Fetching RSS: {feed_url}")

        if "news.google.com" in feed_url:
            session = requests.Session()
            retries = Retry(
                total=5,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                raise_on_status=False,
            )
            adapter = HTTPAdapter(
                max_retries=retries, pool_connections=1, pool_maxsize=1
            )
            session.mount("https://", adapter)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
                "Connection": "close",
            }
            try:
                response = session.get(feed_url, headers=headers, timeout=20)
                response.raise_for_status()
                payload = response.content
            finally:
                session.close()
        else:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url, timeout=30, headers=get_random_headers()
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

        soup = BeautifulSoup(payload, "lxml-xml")
        items = soup.find_all("item")
        feed_build_date = datetime.now(timezone.utc)
        articles = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # We pass the item, the build date, and any specific custom arguments (kwargs) to your processor
            futures = [
                executor.submit(
                    process_item_func, item, feed_build_date=feed_build_date, **kwargs
                )
                for item in items
            ]

            for future in as_completed(futures):
                try:
                    article = future.result()
                except Exception as e:
                    logger.warning(f"Worker failed on item: {e}")
                    continue

                if article is not None:
                    articles.append(article)

        logger.info(f"Parsed {len(articles)} articles from {feed_url}")
        return articles

    except Exception as e:
        logger.error(f"RSS fetch failed [{feed_url}]: {e}")
        return []
