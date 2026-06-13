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


class TheRegisterRSSPipeline:

    SOURCE = "The Register"

    RSS_FEEDS = [
        "https://www.theregister.com/headlines.rss",
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
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(TheRegisterRSSPipeline.HEADERS),
            )
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "lxml")

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

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(TheRegisterRSSPipeline.HEADERS),
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

                    pub_date = item.find("pubDate")
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
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
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
    def run_pipeline(input_data=None):
        try:
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

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"The Register RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
