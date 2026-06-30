import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class FashionTimesRSSPipeline:
    """
    Fashion Times Magazine RSS Pipeline
    """

    SOURCE = "FashionTimes"

    RSS_FEEDS = [
        "https://fashiontimesmagazine.com/feed/",
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

            # remove junk elements
            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    # -----------------------------
    # OPTIONAL: FASHION FILTER
    # -----------------------------
    @staticmethod
    def is_fashion_related(title, categories):
        keywords = [
            "fashion",
            "style",
            "outfit",
            "designer",
            "lawn",
            "celebrity",
            "eid",
            "runway",
            "collection",
            "couture",
        ]

        title_lower = title.lower()

        if any(k in title_lower for k in keywords):
            return True

        for cat in categories:
            if cat.lower() in ["fashion", "lifestyle", "events"]:
                return True

        return False

    # -----------------------------
    # FETCH RSS
    # -----------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Fashion Times RSS: {feed_url}")

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
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")
                    category_elems = item.find_all("category")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    categories = [c.get_text(strip=True) for c in category_elems if c]

                    pub_date = (
                        FashionTimesRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else desc_elem.get_text() if desc_elem else ""
                    )

                    content = FashionTimesRSSPipeline.clean_text(raw_content)

                    # skip low quality posts
                    if len(content) < 150:
                        logger.info(f"Skipped (too short): {title}")
                        continue

                    # filter only fashion-related content
                    if not FashionTimesRSSPipeline.is_fashion_related(
                        title, categories
                    ):
                        logger.info(f"Skipped (not fashion): {title}")
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": "Fashion Times",
                        "language": "en-US",
                        "image": None,
                        "source": FashionTimesRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Fashion",
                        "media_origin": "local",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Fashion Times articles.")
            return articles

        except Exception as e:
            logger.error(f"Fashion Times RSS fetch failed: {e}")
            return []

    # -----------------------------
    # PIPELINE RUN
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in FashionTimesRSSPipeline.RSS_FEEDS:
                articles = FashionTimesRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            # Supabase insert (current year logic)
            result = SupabaseClient.insert_articles_current_year(all_articles)

            return result

        except Exception as e:
            logger.error(f"Fashion Times pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
