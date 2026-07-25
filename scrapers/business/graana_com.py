from config import BUSINESS_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class GraanaRSSPipeline:

    SOURCE = "Graana"

    RSS_FEEDS = [
        "https://www.graana.com/blog/feed/",
    ]

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo:
                    return dt.astimezone(timezone.utc)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_author(author_text: str) -> str:
        if not author_text:
            return "Graana Staff"

        author_text = re.sub(r"\S+@\S+", "", author_text)
        return author_text.strip() or "Graana Staff"

    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            # Remove unwanted tags
            for tag in soup([
                "script", "style", "iframe",
                "noscript", "img", "figure"
            ]):
                tag.decompose()

            # Remove "The post appeared first on..."
            for p in soup.find_all("p"):
                if "appeared first on" in p.get_text():
                    p.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def extract_categories(item):
        categories = item.find_all("category")
        return [cat.get_text(strip=True) for cat in categories if cat]

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Graana RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
                response.raise_for_status()
                payload = response.content

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    creator_elem = item.find("dc:creator")

                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    pub_date = (
                        GraanaRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    if pub_date < seven_days_ago:
                        continue

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else desc_elem.get_text()
                        if desc_elem
                        else ""
                    )

                    content = GraanaRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        continue

                    article = {
                        "id": link_elem.get_text(strip=True),
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title_elem.get_text(strip=True),
                        "authors": GraanaRSSPipeline.clean_author(
                            creator_elem.get_text(strip=True) if creator_elem else ""
                        ),
                        "language": "en-PK",
                        "source": GraanaRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Real Estate",
                        "media_origin": "national",
                        "tags": GraanaRSSPipeline.extract_categories(item),
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process article: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} recent Graana articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in GraanaRSSPipeline.RSS_FEEDS:
                articles = GraanaRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

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

            return SupabaseClient.insert_articles(all_articles, table_name=target_table)

        except Exception as e:
            logger.error(f"Graana RSS pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
