from config import BUSINESS_TABLE
import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class BlockworksRSSPipeline:

    SOURCE = "Blockworks"
    RSS_FEEDS = [
        "https://blockworks.co/feed"
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
    def parse_atom_date(date_str):
        """Parse Atom date into datetime object."""
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
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
        Fetch full article content and author from Blockworks article page.
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
                headers=get_random_headers(BlockworksRSSPipeline.HEADERS)
            )
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "lxml")
            for p in soup.select("div.prose hr ~ p"):
                text = p.get_text(strip=True)
                if len(text) < 40:
                    continue
                if any(x in text for x in ("Blockworks", "©", "All rights reserved")):
                    continue
                paragraphs.append(BlockworksRSSPipeline.clean_content(text))

            if paragraphs:
                content = " ".join(paragraphs)
            author_elem = soup.select_one('a[href^="/author/"]')
            if author_elem:
                author = author_elem.get_text(strip=True)

        except Exception as e:
            logger.warning(f"Failed to fetch Blockworks article {link}: {e}")

        return content, author

    @staticmethod
    def fetch_blockworks_feed(feed_url):
        """Fetch and parse a single Blockworks Atom feed."""
        try:
            logger.info(f"Fetching Blockworks feed: {feed_url}")
            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(BlockworksRSSPipeline.HEADERS)
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml-xml")

            entries = soup.find_all("entry")
            feed_build_date = datetime.now(timezone.utc)

            articles = []
            for entry in entries:
                try:
                    title = entry.find("title").get_text(strip=True)
                    link = entry.find("link")["href"]
                    published = entry.find("published") or entry.find("updated")

                    article_pub_date = (
                        BlockworksRSSPipeline.parse_atom_date(published.get_text())
                        if published else feed_build_date
                    )

                    content, author = BlockworksRSSPipeline.full_description(link)
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
                        "source": BlockworksRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business / Finance / Crypto",
                        "media_origin": "foreign",
                        "tags": "",
                    })

                except Exception as e:
                    logger.warning(f"Failed to process entry: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Blockworks feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all Blockworks feeds concurrently."""
        all_articles = []
        logger.info("Starting Blockworks RSS pipeline (concurrent)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(BlockworksRSSPipeline.fetch_blockworks_feed, feed)
                for feed in BlockworksRSSPipeline.RSS_FEEDS
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
            target_table = BUSINESS_TABLE
            all_articles = []

            for feed_url in BlockworksRSSPipeline.RSS_FEEDS:
                articles = BlockworksRSSPipeline.fetch_blockworks_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"Blockworks RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
