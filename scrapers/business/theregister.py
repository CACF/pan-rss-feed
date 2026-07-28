from config import BUSINESS_TABLE
import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re
import cloudscraper
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class TheRegisterRSSPipeline:

    SOURCE = "The Register"

    RSS_FEEDS = [
        "https://www.theregister.com/headlines.rss",
    ]

    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
        "Referer": "https://www.google.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    ARTICLE_HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    @staticmethod
    def build_headers(base_headers):
        """
        Merge get_random_headers() output with our base headers, but
        FORCE Accept/Referer from base_headers so a randomizer can't
        break API content negotiation (e.g. forcing JSON instead of XML).
        """
        try:
            merged = dict(get_random_headers(base_headers) or {})
        except Exception as e:
            logger.warning(f"get_random_headers failed, falling back: {e}")
            merged = dict(base_headers)

        # Force-critical headers so content negotiation stays correct
        merged["Accept"] = base_headers["Accept"]
        merged["Referer"] = base_headers["Referer"]
        merged["Accept-Language"] = base_headers["Accept-Language"]

        return merged

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
        if not text:
            return ""
        text = re.sub(r"http\S+|www\.\S+", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def full_description(link):
        """
        Fetch full article text + author from The Register page.
        """

        if not link:
            return None, "Unknown"

        try:
            headers = TheRegisterRSSPipeline.build_headers(
                TheRegisterRSSPipeline.ARTICLE_HEADERS
            )

            with cloudscraper.create_scraper() as scraper:
                res = scraper.get(
                    link,
                    timeout=30,
                    headers=headers,
                )
                res.raise_for_status()
                html = res.text

            soup = BeautifulSoup(html, "lxml")

            paragraphs = []

            for p in soup.select("#article #body > p"):
                text = p.get_text(strip=True)

                if len(text) < 40:
                    continue

                if any(x in text for x in ("©", "The Register", "All rights reserved")):
                    continue

                paragraphs.append(
                    TheRegisterRSSPipeline.clean_content(text)
                )

            content = " ".join(paragraphs) if paragraphs else None

            author = "Unknown"
            author_elem = soup.select_one("div.byline_wrap a.byline")
            if author_elem:
                author = author_elem.get_text(strip=True)

            return content, author

        except Exception as e:
            logger.warning(f"Failed to fetch full article {link}: {e}")
            return None, "Unknown"

    @staticmethod
    def fetch_register_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Register RSS feed: {feed_url}")

            headers = TheRegisterRSSPipeline.build_headers(
                TheRegisterRSSPipeline.HEADERS
            )

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=headers,
                )
                response.raise_for_status()
                payload = response.content

            # --- DEBUG LOGGING (remove once confirmed working) ---
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response Content-Type: {response.headers.get('Content-Type')}")
            logger.info(f"Payload length: {len(payload)}")
            logger.info(f"Payload preview: {payload[:300]!r}")
            # --------------------------------------------------------

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            if not items:
                # Fallback: some feeds wrap items differently or the
                # parser choked silently. Try html.parser as a fallback
                # and log a clear warning so we know which path fired.
                logger.warning(
                    "No <item> tags found with lxml-xml parser. "
                    "Retrying with html.parser fallback."
                )
                soup = BeautifulSoup(payload, "html.parser")
                items = soup.find_all("item")
                logger.info(f"Fallback parser found {len(items)} items")

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = item.find("pubdate") or item.find("pubDate")
                    article_pub_date = (
                        TheRegisterRSSPipeline.parse_date(pub_date.get_text())
                        if pub_date
                        else feed_build_date
                    )

                    content, author = TheRegisterRSSPipeline.full_description(link)

                    if not content:
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-gb",
                        "source": TheRegisterRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Technology / Business",
                        "media_origin": "foreign",
                        "tags": "",
                    })

                except Exception as e:
                    logger.warning(f"Failed to process item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.info(f"The Register RSS feed {feed_url} unavailable: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        all_articles = []
        logger.info("Starting The Register RSS pipeline (concurrent)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    TheRegisterRSSPipeline.fetch_register_rss_feed,
                    feed
                )
                for feed in TheRegisterRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Failed to process feed")

        logger.info(f"Total articles processed: {len(all_articles)}")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = TheRegisterRSSPipeline.process_input()

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            if not all_articles:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                    "message": "No articles found",
                }

            result = SupabaseClient.insert_articles(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"The Register RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
