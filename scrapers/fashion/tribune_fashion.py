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


class ExpressTribuneFashionRSSPipeline:
    """
    Express Tribune Fashion RSS Pipeline
    """

    SOURCE = "ExpressTribune"

    RSS_FEEDS = [
        "https://tribune.com.pk/feed/fashion",
    ]

    # --------------------------------
    # DATE PARSER
    # --------------------------------
    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %y %H:%M:%S %z",  # Tue, 23 Jun 26 11:28:41 +0500
            "%a, %d %b %Y %H:%M:%S %z",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Could not parse date: {date_str}")
        return datetime.now(timezone.utc)

    # --------------------------------
    # CLEAN CONTENT
    # --------------------------------
    @staticmethod
    def clean_text(html):
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            for tag in soup(
                [
                    "script",
                    "style",
                    "iframe",
                    "noscript",
                    "img",
                    "figure",
                    "video",
                ]
            ):
                tag.decompose()

            text = soup.get_text(" ")

            text = re.sub(r"http\S+", "", text)
            text = re.sub(r"\s+", " ", text)

            return text.strip()

        except Exception as e:
            logger.warning(f"Failed cleaning content: {e}")
            return html

    # --------------------------------
    # FETCH RSS
    # --------------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Express Tribune RSS: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    headers=get_random_headers(),
                    timeout=30,
                )

                try:
                    response.raise_for_status()
                    xml_data = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(xml_data, "xml")

            items = soup.find_all("item")

            articles = []
            feed_build_date = datetime.now(timezone.utc)

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    # date
                    pub_date_elem = item.find("pubDate")
                    pub_date = (
                        ExpressTribuneFashionRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    # author
                    author_elem = item.find("dc:creator")
                    author = (
                        author_elem.get_text(strip=True)
                        if author_elem
                        else "Express Tribune"
                    )

                    # categories
                    categories = [
                        cat.get_text(strip=True) for cat in item.find_all("category")
                    ]

                    # content
                    content_elem = item.find("content:encoded")
                    description_elem = item.find("description")

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else description_elem.get_text() if description_elem else ""
                    )

                    content = ExpressTribuneFashionRSSPipeline.clean_text(raw_content)

                    # image extraction
                    image_url = None

                    image_container = item.find("image")

                    if image_container:
                        img_tag = image_container.find("img")
                        if img_tag:
                            image_url = img_tag.get("src")

                    # fallback
                    if not image_url:
                        match = re.search(
                            r'<img[^>]+src=["\']([^"\']+)["\']',
                            raw_content,
                            re.I,
                        )
                        if match:
                            image_url = match.group(1)

                    # skip very short articles
                    if len(content) < 200:
                        logger.info(f"Skipped short article: {title}")
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "image": image_url,
                        "source": ExpressTribuneFashionRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Fashion",
                        "media_origin": "local",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Express Tribune articles.")

            return articles

        except Exception as e:
            logger.error(f"RSS fetch failed: {e}")
            return []

    # --------------------------------
    # PIPELINE RUN
    # --------------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or FASHION_TABLE
            all_articles = []

            for feed_url in ExpressTribuneFashionRSSPipeline.RSS_FEEDS:
                articles = ExpressTribuneFashionRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                }

            return SupabaseClient.insert_articles_current_year(all_articles, table_name=target_table)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
