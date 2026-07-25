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


class TechXRSSPipeline:

    SOURCE = "TechX Pakistan"
    RSS_FEEDS = [
        "https://techx.pk/feed/",
    ]

    @staticmethod
    def parse_date(date_str):
        """
        Parse RSS pubDate format:
        Example:
        Tue, 24 Feb 2026 06:32:13 +0000
        """
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception:
            logger.warning(f"Unrecognized RSS date format: {date_str}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML:
        - Remove scripts, styles, iframes, figures, tables
        - Normalize whitespace
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse TechX RSS feed."""
        try:
            logger.info(f"Fetching TechX RSS feed: {feed_url}")

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
                    creator_elem = item.find("dc:creator")
                    category_elems = item.find_all("category")
                    content_elem = item.find("content:encoded")
                    description_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = TechXRSSPipeline.parse_date(
                        pub_date_elem.get_text() if pub_date_elem else None
                    )

                    author_name = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else "TechX Pakistan"
                    )

                    categories = [
                        cat.get_text(strip=True)
                        for cat in category_elems
                        if cat.get_text(strip=True)
                    ]

                    raw_content = ""
                    if content_elem:
                        raw_content = content_elem.get_text()
                    elif description_elem:
                        raw_content = description_elem.get_text()

                    content = TechXRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        logger.info(
                            f"Skipped article '{title}' (content < 200 chars)"
                        )
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author_name,
                        "language": "en-US",
                        "source": TechXRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Technology",
                        "media_origin": "pakistan",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process TechX article entry: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} TechX articles."
            )
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch TechX RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in TechXRSSPipeline.RSS_FEEDS:
                articles = TechXRSSPipeline.fetch_rss_feed(feed_url)
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
            logger.error(f"TechX RSS pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
