from config import BUSINESS_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper
from deep_translator import GoogleTranslator

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ExpressUrduBusinessRSSPipeline:

    SOURCE = "ExpressNews"
    RSS_FEEDS = [
        "https://www.express.pk/feed/business",
    ]

    TRANSLATOR = GoogleTranslator(source="ur", target="en")

    @staticmethod
    def translate_urdu_to_english(text: str) -> str:
        MAX_CHARS = 4500
        if not text:
            return ""

        try:
            chunks = []
            start = 0
            text_length = len(text)

            while start < text_length:
                chunks.append(text[start:start + MAX_CHARS])
                start += MAX_CHARS

            translated_chunks = [
                ExpressUrduBusinessRSSPipeline.TRANSLATOR.translate(chunk)
                for chunk in chunks
            ]

            return " ".join(translated_chunks)

        except Exception as e:
            logger.warning(f"Translation failed: {e}")
            return text

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %z",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript", "img"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Express Urdu RSS feed: {feed_url}")

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
                    desc_elem = item.find("description")
                    content_elem = item.find("content:encoded")

                    if not title_elem or not link_elem:
                        continue

                    urdu_title = title_elem.get_text(strip=True)
                    urdu_description = desc_elem.get_text(strip=True) if desc_elem else ""
                    urdu_content = content_elem.get_text() if content_elem else urdu_description

                    clean_content = ExpressUrduBusinessRSSPipeline.clean_content(urdu_content)

                    if len(clean_content) < 200:
                        continue

                    title_en = ExpressUrduBusinessRSSPipeline.translate_urdu_to_english(urdu_title)
                    description_en = ExpressUrduBusinessRSSPipeline.translate_urdu_to_english(urdu_description)
                    content_en = ExpressUrduBusinessRSSPipeline.translate_urdu_to_english(clean_content)

                    pub_date = (
                        ExpressUrduBusinessRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    link = link_elem.get_text(strip=True)

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title_en,
                        "summary": description_en,
                        "authors": "Express News Urdu Desk",
                        "language": "en-US",
                        "source": ExpressUrduBusinessRSSPipeline.SOURCE,
                        "content": content_en,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    })

                except Exception as e:
                    logger.warning(f"Failed to process Express Urdu article item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Express Urdu business articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Express Urdu RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = BUSINESS_TABLE
            all_articles = []

            for feed_url in ExpressUrduBusinessRSSPipeline.RSS_FEEDS:
                articles = ExpressUrduBusinessRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            return SupabaseClient.insert_articles(all_articles, table_name=target_table)

        except Exception as e:
            logger.error(f"Express Urdu pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
