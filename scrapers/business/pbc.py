from config import BUSINESS_TABLE
import uuid
import logging
import re
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import requests

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class PBCNewsRSSPipeline:
    """
    Pakistan Business Council RSS feed pipeline
    """

    SOURCE = "Pakistan Business Council"
    RSS_FEEDS = [
        "https://www.pbc.org.pk/news/feed/",  # PBC RSS
    ]
    DAYS_TO_KEEP = 7  # Keep only last 7 days of articles

    @staticmethod
    def parse_date(date_str):
        """Parse PBC RSS date format to datetime object."""
        try:
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Convert HTML to clean plain text and remove links."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")

            # Remove unwanted elements
            for tag in soup(["script", "style", "iframe", "noscript", "form", "aside"]):
                tag.decompose()

            # Remove anchor tags but keep visible text
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()

            # Extract text
            text = soup.get_text(separator=" ", strip=True)

            # Remove any remaining URLs
            text = re.sub(r"http\S+|www\.\S+", "", text)

            # Normalize whitespace
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse PBC RSS feed."""
        try:
            logger.info(f"Fetching PBC RSS feed: {feed_url}")
            response = requests.get(feed_url, timeout=30, headers=get_random_headers())
            response.raise_for_status()
            payload = response.content
            response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)
            articles = []

            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=PBCNewsRSSPipeline.DAYS_TO_KEEP)

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    author_elem = item.find("dc:creator")
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")
                    pub_date_elem = item.find("pubDate")

                    if not title_elem or not link_elem:
                        continue

                    pub_date = (
                        PBCNewsRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem else datetime.now(timezone.utc)
                    )

                    # Skip articles older than DAYS_TO_KEEP
                    if pub_date < seven_days_ago:
                        continue

                    # Clean content
                    content = ""
                    if content_elem:
                        content = PBCNewsRSSPipeline.clean_content(content_elem.get_text())
                    elif desc_elem:
                        content = PBCNewsRSSPipeline.clean_content(desc_elem.get_text())

                    # Skip articles with very short content
                    if len(content) < 200:
                        logger.info(f"Skipping short article ({len(content)} chars): {link_elem.get_text(strip=True)}")
                        continue

                    article = {
                        "id": link_elem.get_text(strip=True),
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title_elem.get_text(strip=True),
                        "authors": author_elem.get_text(strip=True) if author_elem else "Pakistan Business Council",
                        "language": "en-US",
                        "source": PBCNewsRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)
                except Exception as e:
                    logger.warning(f"Failed to process PBC article item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} valid articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch PBC RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in PBCNewsRSSPipeline.RSS_FEEDS:
                articles = PBCNewsRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles, table_name=table_name)

            return result

        except Exception as e:
            logger.error(f"PBC News RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
