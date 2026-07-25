from config import BUSINESS_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class AajTVBusinessRSSPipeline:
    """
    Aaj TV English RSS feed pipeline — Business & Economy
    Flask-compatible, single-threaded, production-safe
    """

    SOURCE = "AajTV"
    RSS_FEEDS = [
        "https://english.aaj.tv/feeds/business-economy/",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime (UTC normalized)."""
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
        """
        Clean author field like:
        'none@none.com (Reuters)' -> 'Reuters'
        'none@none.com (Web Desk)' -> 'Web Desk'
        """
        if not author_text:
            return "Aaj TV Business Desk"

        match = re.search(r"\((.*?)\)", author_text)
        if match:
            return match.group(1).strip()

        author_text = re.sub(r"\S+@\S+", "", author_text)
        return author_text.strip() or "Aaj TV Business Desk"

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML into readable text:
        - Remove images, figures
        - Remove scripts, styles, iframes
        - Normalize whitespace
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(
                ["script", "style", "iframe", "noscript", "img", "figure"]
            ):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse Aaj TV Business RSS feed."""
        try:
            logger.info(f"Fetching Aaj TV RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
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
                    pub_date_elem = item.find("pubDate")
                    author_elem = item.find("author")

                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        AajTVBusinessRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else desc_elem.get_text()
                        if desc_elem
                        else ""
                    )

                    content = AajTVBusinessRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        logger.info(
                            f"Skipped article '{title}' (content < 200 chars)"
                        )
                        continue

                    raw_author = (
                        author_elem.get_text(strip=True)
                        if author_elem
                        else ""
                    )
                    authors = AajTVBusinessRSSPipeline.clean_author(
                        raw_author
                    )

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": authors,
                        "language": "en-US",
                        "source": AajTVBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process Aaj TV article item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} Aaj TV business articles."
            )
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Aaj TV RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in AajTVBusinessRSSPipeline.RSS_FEEDS:
                articles = AajTVBusinessRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}
            
            result = SupabaseClient.insert_articles(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"Aaj TV RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
