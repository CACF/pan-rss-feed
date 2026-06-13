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


class TechCrunchRSSPipeline:

    SOURCE = "TechCrunch"
    RSS_FEEDS = [
        "https://techcrunch.com/feed/"
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
        """Parse RSS pubDate into datetime object."""
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
        """Clean text: remove URLs, extra spaces."""
        if not text:
            return ""
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @staticmethod
    def full_description(link):
        """
        Fetch full article content and author from TechCrunch article page.
        """
        content = None
        author = "Unknown"
        paragraphs = []

        if not link:
            return None, author

        try:
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(TechCrunchRSSPipeline.HEADERS)
            )
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "lxml")

            for p in soup.select("div.entry-content.wp-block-post-content > p.wp-block-paragraph"):
                text = p.get_text(strip=True)

                if len(text) < 30:
                    continue
                if any(x in text for x in ("©", "TechCrunch", "All rights reserved")):
                    continue

                paragraphs.append(
                    TechCrunchRSSPipeline.clean_content(text)
                )

            if paragraphs:
                content = " ".join(paragraphs)
            author_elem = soup.select_one("div.article-hero__authors a.wp-block-tc23-author-card-name__link")
            if author_elem:
                author = author_elem.get_text(strip=True)

        except Exception as e:
            logger.warning(f"Failed to fetch full article {link}: {e}")
        return content, author

    @staticmethod
    def fetch_techcrunch_rss_feed(feed_url):
        """Fetch and parse a single TechCrunch RSS feed."""
        try:
            logger.info(f"Fetching TechCrunch RSS feed: {feed_url}")
            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(TechCrunchRSSPipeline.HEADERS)
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
                        TechCrunchRSSPipeline.parse_date(pub_date.get_text())
                        if pub_date else feed_build_date
                    )

                    content, author = TechCrunchRSSPipeline.full_description(link)

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
                        "source": TechCrunchRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Technology",
                        "media_origin": "foreign",
                        "tags": "",
                    })

                except Exception as e:
                    logger.warning(f"Failed to process item {item}: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all TechCrunch RSS feeds concurrently."""
        all_articles = []
        logger.info("Starting TechCrunch RSS pipeline (concurrent)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    TechCrunchRSSPipeline.fetch_techcrunch_rss_feed,
                    feed
                )
                for feed in TechCrunchRSSPipeline.RSS_FEEDS
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
            all_articles = []

            for feed_url in TechCrunchRSSPipeline.RSS_FEEDS:
                articles = TechCrunchRSSPipeline.fetch_techcrunch_rss_feed(feed_url)
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
            logger.error(f"TechCrunch RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
