from config import BUSINESS_TABLE
import uuid
import logging
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import requests

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class WSJRSSPipeline:

    SOURCE = "WSJ US Business"
    RSS_FEEDS = [
        "https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness",  # US Business RSS
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS date format to datetime object."""
        try:
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
            dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Clean HTML content to plain text, removing links and extra spaces."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()

            for a_tag in soup.find_all("a"):
                a_tag.unwrap()
            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\S+", "", text)
            text = " ".join(text.split())

            return text.strip()
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse a single WSJ RSS feed."""
        try:
            logger.info(f"Fetching WSJ RSS feed: {feed_url}")
            response = requests.get(feed_url, timeout=30, headers=get_random_headers())
            try:
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "lxml-xml")
            finally:
                response.close()
            items = soup.find_all("item")
            feed_build_date_str = soup.find("lastBuildDate")
            feed_build_date = (
                WSJRSSPipeline.parse_date(feed_build_date_str.text)
                if feed_build_date_str
                else None
            )

            articles = []
            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    desc_elem = item.find("description")
                    author_elem = item.find("dc:creator")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    pub_date = (
                        WSJRSSPipeline.parse_date(item.find("pubDate").get_text())
                        if item.find("pubDate")
                        else None
                    )
                    author = (
                        author_elem.get_text(strip=True) if author_elem else "Unknown"
                    )
                    content = (
                        WSJRSSPipeline.clean_content(desc_elem.get_text())
                        if desc_elem
                        else ""
                    )
                    if len(content) < 200:
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": WSJRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "foreign",
                        "tags": [],
                        "feedBuildDate": datetime.now(timezone.utc),
                    }

                    articles.append(article)
                except Exception as e:
                    logger.warning(f"Failed to process WSJ article item: {e}")
                    continue

            logger.info(f"Successfully parsed {len(articles)} articles from {feed_url}")
            return articles
        except Exception as e:
            logger.error(f"Failed to fetch WSJ RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in WSJRSSPipeline.RSS_FEEDS:
                articles = WSJRSSPipeline.fetch_rss_feed(feed_url)
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
            logger.error(f"Tribune RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
