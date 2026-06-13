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


class TheVergeRSSPipeline:

    SOURCE = "The Verge"
    RSS_FEEDS = [
        "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
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
        if not date_str:
            return datetime.now(timezone.utc)
        try:
            dt = datetime.fromisoformat(date_str.strip())
            if dt.tzinfo:
                return dt.astimezone(timezone.utc)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(text):
        if not text:
            return ""
        text = re.sub(r"<.*?>", "", text)
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @staticmethod
    def full_description(link):
        """Fetch full content and author from The Verge article page."""
        if not link:
            return None, None
        try:
            res = requests.get(link, timeout=30, headers=get_random_headers(TheVergeRSSPipeline.HEADERS))
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "lxml")

            # Extract article content
            paragraphs = [p.get_text(strip=True) for p in soup.select(
                "#zephr-anchor .duet--article--article-body-component p.duet--article--dangerously-set-cms-markup"
            )]
            paragraphs = [p for p in paragraphs if len(p) > 30]
            content = " ".join(paragraphs) if paragraphs else None

            # Extract author (direct text only to avoid duplicates)
            authors = "The Verge"  # default
            author_span = soup.select_one("span[id^='follow-author-author_byline'] > span:last-child")
            if author_span:
                texts = [t.strip() for t in author_span.find_all(string=True, recursive=False)]
                filtered = list(filter(None, texts))
                if filtered:
                    authors = ", ".join(filtered)

            return content, authors

        except Exception as e:
            logger.warning(f"Failed to fetch full article {link}: {e}")
            return None, None

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching The Verge RSS feed: {feed_url}")
            response = requests.get(feed_url, timeout=30, headers=get_random_headers(TheVergeRSSPipeline.HEADERS))
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml-xml")

            # Handle Atom (<entry>) and RSS (<item>)
            entries = soup.find_all("entry") or soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for entry in entries:
                try:
                    title_elem = entry.find("title")
                    link_elem = entry.find("link")
                    published_elem = entry.find("published") or entry.find("updated") or entry.find("pubDate")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get("href") if link_elem.has_attr("href") else link_elem.get_text(strip=True)
                    pub_date = TheVergeRSSPipeline.parse_date(
                        published_elem.get_text() if published_elem else None
                    )

                    content, authors = TheVergeRSSPipeline.full_description(link)
                    if not content or len(content) < 50:
                        continue

                    categories = [cat.get("term") for cat in entry.find_all("category") if cat.get("term")]

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": authors,
                        "language": "en-US",
                        "source": TheVergeRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Technology",
                        "media_origin": "international",
                        "tags": categories,
                    })

                except Exception as e:
                    logger.warning(f"Failed to process entry {entry}: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        all_articles = []
        logger.info("Starting The Verge RSS pipeline (concurrent)")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(TheVergeRSSPipeline.fetch_rss_feed, feed)
                       for feed in TheVergeRSSPipeline.RSS_FEEDS]
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
            all_articles = TheVergeRSSPipeline.process_input()

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
            logger.error(f"The Verge RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }