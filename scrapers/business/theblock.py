from config import BUSINESS_TABLE
import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re

try:
    from curl_cffi import requests

    HAS_CURL_CFFI = True
except ImportError:
    import requests
    import cloudscraper

    HAS_CURL_CFFI = False

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class TheBlockRSSPipeline:
    """
    The Block RSS feed pipeline.
    RSS -> link only
    Scrape content + author from article page (Cloudflare-safe)
    """

    SOURCE = "The Block"
    RSS_FEEDS = ["https://www.theblock.co/rss.xml"]

    BASE_HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    if HAS_CURL_CFFI:
        session = requests.Session(impersonate="chrome120")
        session.headers.update(BASE_HEADERS)
    else:
        session = cloudscraper.create_scraper()
        session.headers.update(BASE_HEADERS)

    @staticmethod
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S %z",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(text):
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @classmethod
    def full_description(cls, link):
        """
        Fetch full article content + author (Cloudflare-safe)
        """
        content = None
        author = "Unknown"
        paragraphs = []

        if not link:
            return None, author

        clean_link = link.split("?")[0]
        amp_link = clean_link.rstrip("/") + "/amp"

        try:
            res = cls.session.get(amp_link, timeout=25)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "lxml")

            for p in soup.select(".dynamic-content > p"):
                text = p.get_text(strip=True)
                if len(text) < 40:
                    continue
                if any(x in text for x in ("©", "The Block", "All rights reserved")):
                    continue
                paragraphs.append(cls.clean_content(text))

            if paragraphs:
                content = " ".join(paragraphs)

            author_elem = soup.select_one("div.bylines a")
            if author_elem:
                author = author_elem.get_text(strip=True)

        except Exception as e:
            logger.warning(f"Article scrape failed {amp_link}: {e}")

        return content, author

    @classmethod
    def fetch_theblock_rss_feed(cls, feed_url):
        logger.info(f"Fetching The Block RSS feed: {feed_url}")

        try:
            with cloudscraper.create_scraper() as scraper:
                res = scraper.get(feed_url, timeout=15, headers=get_random_headers())
                res.raise_for_status()
                payload = res.content
        except Exception as e:
            logger.info(f"The Block RSS feed {feed_url} unavailable (Cloudflare 403 / network restriction): {e}")
            return []

        soup = BeautifulSoup(res.content, "lxml-xml")
        items = soup.find_all("item")
        feed_build_date = datetime.now(timezone.utc)

        articles = []

        for item in items:
            try:
                title = item.find("title").get_text(strip=True)
                link = item.find("link").get_text(strip=True)
                pub_date = item.find("pubDate")
                article_pub_date = (
                    cls.parse_date(pub_date.get_text()) if pub_date else feed_build_date
                )

                content, author = cls.full_description(link)
                if not content:
                    continue

                articles.append(
                    {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": cls.SOURCE,
                        "content": content,
                        "genre": "Crypto",
                        "media_origin": "foreign",
                        "tags": "",
                    }
                )

            except Exception as e:
                logger.warning(f"Item failed: {e}")

        logger.info(f"Parsed {len(articles)} articles from {feed_url}")
        return articles

    @classmethod
    def process_input(cls, input_data=None):
        logger.info("Starting The Block RSS pipeline (concurrent)")
        all_articles = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(cls.fetch_theblock_rss_feed, feed)
                for feed in cls.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Feed processing failed")

        return all_articles

    @classmethod
    def run_pipeline(cls, input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in cls.RSS_FEEDS:
                articles = cls.fetch_theblock_rss_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"The Block RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
