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


class PlotistanRSSPipeline:

    SOURCE = "Plotistan.pk"

    RSS_FEEDS = [
        "https://plotistan.pk/feed/",
    ]

    # CONFIGURABLE
    DAYS_BACK = None  # Set to integer (e.g., 30) to enable date filtering
    MIN_CONTENT_LENGTH = 200

    # -----------------------------------------------------
    # DATE PARSER
    # -----------------------------------------------------
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
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    # -----------------------------------------------------
    # CONTENT CLEANER
    # -----------------------------------------------------
    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            # Remove unwanted tags
            for tag in soup([
                "script", "style", "iframe", "noscript",
                "img", "figure", "svg", "form"
            ]):
                tag.decompose()

            # Remove rating blocks
            for div in soup.find_all("div", class_=re.compile("kk-star-ratings")):
                div.decompose()

            # Remove WP footer
            for p in soup.find_all("p"):
                if "appeared first on" in p.get_text().lower():
                    p.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            cleaned = " ".join(text.split())

            return cleaned

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    # -----------------------------------------------------
    # CATEGORY EXTRACTOR
    # -----------------------------------------------------
    @staticmethod
    def extract_categories(item):
        categories = item.find_all("category")
        return [
            cat.get_text(strip=True)
            for cat in categories
            if cat and cat.get_text(strip=True)
        ]

    # -----------------------------------------------------
    # FETCH RSS
    # -----------------------------------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Plotistan RSS feed: {feed_url}")

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

            if not items:
                logger.warning("No RSS items found.")
                return []

            logger.info(f"Found {len(items)} items in feed.")

            feed_build_date = datetime.now(timezone.utc)
            date_cutoff = None

            if PlotistanRSSPipeline.DAYS_BACK:
                date_cutoff = feed_build_date - timedelta(days=PlotistanRSSPipeline.DAYS_BACK)

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
                        logger.warning("Skipping item: missing title or link.")
                        continue

                    link = link_elem.get_text(strip=True)
                    pub_date = (
                        PlotistanRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem else datetime.now(timezone.utc)
                    )

                    # Optional date filtering
                    if date_cutoff and pub_date < date_cutoff:
                        logger.info(f"Skipping old article: {link}")
                        continue

                    raw_content = (
                        content_elem.get_text()
                        if content_elem else
                        desc_elem.get_text()
                        if desc_elem else ""
                    )

                    content = PlotistanRSSPipeline.clean_content(raw_content)

                    logger.debug(f"Content length for {link}: {len(content)}")

                    if len(content) < PlotistanRSSPipeline.MIN_CONTENT_LENGTH:
                        logger.info(f"Skipping short article: {link}")
                        continue

                    article = {
                        "id": link,  # Safe dedupe key
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title_elem.get_text(strip=True),
                        "authors": creator_elem.get_text(strip=True)
                        if creator_elem else "Plotistan Staff",
                        "language": "en-US",
                        "source": PlotistanRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Real Estate",
                        "media_origin": "pakistan",
                        "tags": PlotistanRSSPipeline.extract_categories(item),
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process article: {e}")
                    continue

            logger.info(f"Prepared {len(articles)} articles for insertion.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Plotistan RSS feed: {e}")
            return []

    # -----------------------------------------------------
    # MAIN PIPELINE
    # -----------------------------------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in PlotistanRSSPipeline.RSS_FEEDS:
                articles = PlotistanRSSPipeline.fetch_rss_feed(feed_url)
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
