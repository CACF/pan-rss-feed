from config import FASHION_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class DivaFashionRSSPipeline:
    """
    Diva Magazine Fashion RSS Pipeline
    """

    SOURCE = "DivaMagazine"

    RSS_FEEDS = [
        "https://www.divaonline.com.pk/feed/",
    ]

    # -----------------------------
    # DATE PARSER
    # -----------------------------
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
                return (
                    dt.astimezone(timezone.utc)
                    if dt.tzinfo
                    else dt.replace(tzinfo=timezone.utc)
                )
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    # -----------------------------
    # CLEAN CONTENT
    # -----------------------------
    @staticmethod
    def clean_text(html):
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            # remove noise
            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    # -----------------------------
    # FASHION FILTER
    # -----------------------------
    @staticmethod
    def is_fashion_related(title, categories):
        keywords = [
            "fashion",
            "style",
            "bridal",
            "outfit",
            "designer",
            "collection",
            "trend",
            "runway",
            "beauty",
            "makeup",
            "wedding",
            "couture",
            "clothing",
        ]

        title_lower = title.lower()

        if any(k in title_lower for k in keywords):
            return True

        # Diva uses categories like Fashion, Entertainment, etc.
        allowed_categories = {"fashion", "beauty", "style"}

        for cat in categories:
            if cat.lower() in allowed_categories:
                return True

        return False

    # -----------------------------
    # FETCH RSS
    # -----------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Diva RSS: {feed_url}")

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

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title = (
                        item.find("title").get_text(strip=True)
                        if item.find("title")
                        else None
                    )
                    link = (
                        item.find("link").get_text(strip=True)
                        if item.find("link")
                        else None
                    )

                    if not title or not link:
                        continue

                    categories = [
                        c.get_text(strip=True) for c in item.find_all("category") if c
                    ]

                    pub_date = (
                        DivaFashionRSSPipeline.parse_date(
                            item.find("pubDate").get_text()
                        )
                        if item.find("pubDate")
                        else datetime.now(timezone.utc)
                    )

                    content_raw = (
                        item.find("content:encoded").get_text()
                        if item.find("content:encoded")
                        else (
                            item.find("description").get_text()
                            if item.find("description")
                            else ""
                        )
                    )

                    content = DivaFashionRSSPipeline.clean_text(content_raw)

                    author = (
                        item.find("dc:creator").get_text(strip=True)
                        if item.find("dc:creator")
                        else "Diva Magazine"
                    )

                    # skip short articles
                    if len(content) < 200:
                        continue

                    # fashion filter
                    if not DivaFashionRSSPipeline.is_fashion_related(title, categories):
                        continue

                    articles.append(
                        {
                            "id": link,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "authors": author,
                            "language": "en-US",
                            "image": None,
                            "source": DivaFashionRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Fashion",
                            "media_origin": "local",
                            "tags": categories,
                        }
                    )

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Diva articles.")
            return articles

        except Exception as e:
            logger.error(f"Diva RSS fetch failed: {e}")
            return []

    # -----------------------------
    # RUN PIPELINE
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or FASHION_TABLE
            all_articles = []

            for feed_url in DivaFashionRSSPipeline.RSS_FEEDS:
                all_articles.extend(DivaFashionRSSPipeline.fetch_rss_feed(feed_url))

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles_current_year(all_articles, table_name=target_table)

        except Exception as e:
            logger.error(f"Diva pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
