import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re
import requests
from app.utilities import MongoDBClient, get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class APPBusinessRSSPipeline:

    SOURCE = "Associated Press Of Pakistan"

    RSS_FEEDS = [
        "https://www.app.com.pk/business/feed/",
        "https://www.app.com.pk/sports/feed/"
    ]

    HEADERS = {
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
    def parse_rss_date(date_str):
        """Parse RSS pubDate into UTC datetime."""
        try:
            return datetime.strptime(
                date_str, "%a, %d %b %Y %H:%M:%S %z"
            ).astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(text):
        """Clean text by removing URLs and extra spaces."""
        if not text:
            return ""
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @staticmethod
    def full_description(link):

        if not link:
            return None

        try:
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(APPBusinessRSSPipeline.HEADERS)
            )
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "lxml")

            paragraphs = []

            for p in soup.select("div.entry-content > p"):

                text = p.get_text(strip=True)

                if len(text) < 40:
                    continue

                if any(x in text for x in (
                    "Associated Press Of Pakistan",
                    "first appeared on",
                    "owns the property"
                )):
                    continue

                paragraphs.append(
                    APPBusinessRSSPipeline.clean_content(text)
                )

            if paragraphs:
                return " ".join(paragraphs)

        except Exception as e:
            logger.warning(f"Failed to fetch APP article {link}: {e}")

        return None

    @staticmethod
    def fetch_app_feed(feed_url):
        """Fetch and parse APP Business RSS feed."""
        try:
            logger.info(f"Fetching APP feed: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(APPBusinessRSSPipeline.HEADERS)
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "lxml-xml")

            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for item in items:
                try:
                    title = item.find("title").get_text(strip=True)
                    link = item.find("link").get_text(strip=True)

                    pub_date_tag = item.find("pubDate")
                    article_pub_date = (
                        APPBusinessRSSPipeline.parse_rss_date(pub_date_tag.get_text())
                        if pub_date_tag else feed_build_date
                    )

                    author_tag = item.find("dc:creator")
                    author = author_tag.get_text(strip=True) if author_tag else "Unknown"

                    content = APPBusinessRSSPipeline.full_description(link)

                    if not content:
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": APPBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": (
                                    "Business"
                                    if "business" in feed_url.lower()
                                    else "Sports"
                                    if "sports" in feed_url.lower()
                                    else ""
                                ),
                        "media_origin": "local",
                        "tags": [],
                    })

                except Exception as e:
                    logger.warning(f"Failed to process APP item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch APP feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all APP feeds concurrently."""
        all_articles = []
        logger.info("Starting APP Business RSS pipeline")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(APPBusinessRSSPipeline.fetch_app_feed, feed)
                for feed in APPBusinessRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Failed to process feed")

        logger.info(f"Total APP articles processed: {len(all_articles)}")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = APPBusinessRSSPipeline.process_input()

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
            logger.error(f"APP RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }