from config import SPORTS_TABLE
import uuid
import logging
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import re
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class TradeChronicleSportsRSSPipeline:

    SOURCE = "Trade Chronicle"
    RSS_FEEDS = [
        "https://tradechronicle.com/category/health-sports/feed/",
    ]
    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }
    MAX_WORKERS = 5

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime UTC."""
        formats = ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_text(text):
        """Clean string: collapse multiple spaces/newlines into a single space."""
        if not text:
            return ""
        return " ".join(text.split())

    @staticmethod
    def full_description(link):
        if not link:
            return None, "Unknown"

        try:
            res = requests.get(
                link,
                timeout=15,
                headers=get_random_headers(TradeChronicleSportsRSSPipeline.HEADERS)
            )
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "html.parser")

            author = "Unknown"
            author_elem = (
                soup.select_one("span.author.vcard a")
                or soup.select_one("a[rel='author']")
                or soup.select_one("span.byline a")
            )
            if author_elem:
                author = author_elem.get_text(strip=True)

            paragraphs = []
            for p in soup.select("div.entry-content > p"):
                text = p.get_text(strip=True)
                if len(text) < 30:
                    continue
                if any(x in text.lower() for x in [
                    "appeared first on",
                    "all rights reserved",
                    "copyright",
                    "trade chronicle",
                ]):
                    continue
                paragraphs.append(TradeChronicleSportsRSSPipeline.clean_text(text))

            if paragraphs:
                return " ".join(paragraphs), author

        except Exception as e:
            logger.warning(f"Failed to fetch article body {link}: {e}")

        return None, "Unknown"

    @staticmethod
    def fetch_rss_feed(feed_url):
        logger.info(f"Fetching Trade Chronicle Sports RSS feed: {feed_url}")
        try:
            res = requests.get(
                feed_url,
                timeout=20,
                headers=get_random_headers(TradeChronicleSportsRSSPipeline.HEADERS)
            )
            res.raise_for_status()

            soup = BeautifulSoup(res.content, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    if not title_elem or not link_elem:
                        continue

                    title = TradeChronicleSportsRSSPipeline.clean_text(title_elem.get_text())
                    link = link_elem.get_text(strip=True)

                    pub_date_tag = item.find("pubDate")
                    article_pub_date = (
                        TradeChronicleSportsRSSPipeline.parse_date(pub_date_tag.get_text())
                        if pub_date_tag
                        else feed_build_date
                    )

                    content, author = TradeChronicleSportsRSSPipeline.full_description(link)
                    if not content or len(content) < 100:
                        continue

                    sports_keywords = [
                        "sport", "cricket", "football", "soccer", "hockey", "match", "league",
                        "tournament", "player", "coach", "stadium", "trophy", "champion",
                        "fifa", "olympic", "wrestl", "race", "boxing", "fitness", "mpl", "afridi"
                    ]
                    text_to_check = (title + " " + content).lower()
                    if not any(sk in text_to_check for sk in sports_keywords):
                        logger.info(f"Skipping non-sports health article: '{title}' ({link})")
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": TradeChronicleSportsRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Sports",
                        "media_origin": "local",
                        "tags": "",
                    })

                except Exception as e:
                    logger.warning(f"Item parse failed: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"RSS fetch failed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        all_articles = []
        logger.info("Starting Trade Chronicle Sports RSS pipeline")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=TradeChronicleSportsRSSPipeline.MAX_WORKERS
        ) as executor:
            futures = [
                executor.submit(TradeChronicleSportsRSSPipeline.fetch_rss_feed, feed)
                for feed in TradeChronicleSportsRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Feed processing exception")

        return all_articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = SPORTS_TABLE
            all_articles = TradeChronicleSportsRSSPipeline.process_input()

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(all_articles, table_name=target_table, category="sports")
            return result

        except Exception as e:
            logger.error(f"Trade Chronicle Sports RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
