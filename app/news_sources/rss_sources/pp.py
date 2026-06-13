import uuid
import logging
import time
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import requests
from app.utilities import MongoDBClient, get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ProfitPakistanTodayRSSPipeline:
    """
    Profit by Pakistan Today RSS feed pipeline that fetches, parses,
    and stores Profit news articles.
    """

    SOURCE = "Profit by Pakistan Today"
    RSS_FEEDS = [
        # "https://profit.pakistantoday.com.pk/feed/",
        "https://propakistani.pk/category/sports/feed/"
    ]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate format to datetime."""
        if not date_str:
            return datetime.now(timezone.utc)
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Convert HTML to clean plain text (no links, no scripts)."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style", "aside", "figure", "iframe"]):
                tag.decompose()

            for a in soup.find_all("a"):
                a.unwrap()

            text = soup.get_text(separator=" ", strip=True)
            text = " ".join(
                word for word in text.split() if not word.startswith("http")
            )

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    @staticmethod
    def fetch_profit_rss_feed(feed_url):
        """Fetch and parse a single Profit RSS feed."""
        try:
            logger.info(f"Fetching Profit RSS feed: {feed_url}")
            response = requests.get(
                feed_url, timeout=30, headers=get_random_headers(ProfitPakistanTodayRSSPipeline.headers)
            )
            try:
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            if not items:
                logger.warning(f"No items found in feed: {feed_url}")
                return []

            articles = []
            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    desc_elem = item.find("description")
                    pub_date_elem = item.find("pubDate")
                    category_elem = item.find("category")
                    author_elem = item.find("dc:creator")
                    content_encoded_elem = item.find("content:encoded")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    pub_date = pub_date_elem.get_text(strip=True) if pub_date_elem else ""
                    category = category_elem.get_text(strip=True) if category_elem else "Business"
                    author = author_elem.get_text(strip=True) if author_elem else "Profit Desk"

                    content_html = (
                        content_encoded_elem.get_text() if content_encoded_elem else
                        (desc_elem.get_text() if desc_elem else "")
                    )
                    content = ProfitPakistanTodayRSSPipeline.clean_content(content_html)
                    if len(content) < 200:
                        logger.info(f"Skipped article '{title}' due to content length < 200 chars")
                        continue
                    article = {
                        "id": link, 
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": ProfitPakistanTodayRSSPipeline.parse_date(pub_date),
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": ProfitPakistanTodayRSSPipeline.SOURCE,
                        "content": content,
                        "genre":  (
                                    "Business"
                                    if "business" in feed_url.lower()
                                    else "Sports"
                                    if "sports" in feed_url.lower()
                                    else ""
                                ),
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process Profit article: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Profit RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all RSS feeds concurrently."""
        try:
            logger.info("Starting Profit RSS pipeline (concurrent)")
            all_articles = []
            max_workers = 5

            def _fetch(feed):
                return ProfitPakistanTodayRSSPipeline.fetch_profit_rss_feed(feed)

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_feed = {
                    executor.submit(_fetch, feed): feed
                    for feed in ProfitPakistanTodayRSSPipeline.RSS_FEEDS
                }

                for future in concurrent.futures.as_completed(future_to_feed):
                    feed = future_to_feed[future]
                    try:
                        articles = future.result()
                        all_articles.extend(articles)
                        logger.info(f"Feed processed: {feed} -> {len(articles)} articles")
                    except Exception:
                        logger.exception(f"Feed failed: {feed}")
                        continue

            logger.info(f"Profit pipeline processed {len(all_articles)} total articles")
            return all_articles

        except Exception as e:
            logger.error(f"Profit RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in ProfitPakistanTodayRSSPipeline.RSS_FEEDS:
                articles = ProfitPakistanTodayRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"Tribune RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }