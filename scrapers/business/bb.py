from config import BUSINESS_TABLE
import uuid
import logging
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class BloombergRSSPipeline:

    SOURCE = "Bloomberg"
    RSS_FEEDS = [
        "https://feeds.bloomberg.com/business/news.rss"
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS date format to datetime object."""
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Convert HTML (if present) to clean plain text and remove URLs."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()

            text = soup.get_text()
            text = re.sub(r"http\S+|www\.\S+", "", text)
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            cleaned_text = " ".join(chunk for chunk in chunks if chunk)
            return cleaned_text.strip()
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    @staticmethod
    def fetch_bloomberg_rss_feed(feed_url):
        """Fetch and parse a single Bloomberg RSS feed."""
        try:
            logger.info(f"Fetching Bloomberg RSS feed: {feed_url}")
            response = requests.get(feed_url, timeout=30, headers=get_random_headers())
            try:
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()
            soup = BeautifulSoup(payload, "lxml-xml")

            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            articles = []
            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    desc_elem = item.find("description")
                    creator_elem = item.find("dc:creator")
                    category_elems = item.find_all("category")
                    pub_date_elem = item.find("pubDate")
                    guid_elem = item.find("guid")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text().strip()
                    link = link_elem.get_text().strip()
                    description_text = desc_elem.get_text().strip() if desc_elem else ""
                    content = BloombergRSSPipeline.clean_content(description_text)

                    if not content or len(content) < 200:
                        logger.info(f"Skipping short article: '{title}' (length: {len(content)})")
                        continue

                    author = creator_elem.get_text().strip() if creator_elem else "Unknown"
                    tags = [cat.get_text().strip() for cat in category_elems if cat.get_text()]

                    article_pub_date = (
                        BloombergRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    article_id = link or (guid_elem.get_text().strip() if guid_elem else str(uuid.uuid4()))

                    article = {
                        "id": article_id,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": BloombergRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "foreign",
                        "tags": tags,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process Bloomberg article: {e}")
                    continue

            logger.info(f"Successfully parsed {len(articles)} valid articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Bloomberg RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all Bloomberg RSS feeds."""
        try:
            logger.info("Starting Bloomberg RSS pipeline processing")
            all_articles = []
            for feed_url in BloombergRSSPipeline.RSS_FEEDS:
                articles = BloombergRSSPipeline.fetch_bloomberg_rss_feed(feed_url)
                all_articles.extend(articles)
            logger.info(
                f"Bloomberg RSS pipeline processed {len(all_articles)} total articles"
            )
            return all_articles
        except Exception as e:
            logger.error(f"Bloomberg RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in BloombergRSSPipeline.RSS_FEEDS:
                articles = BloombergRSSPipeline.fetch_bloomberg_rss_feed(feed_url)
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
            logger.error(f"Bloomberg RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
