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


class TheRegisterRSSPipeline:

    SOURCE = "The Register"
    RSS_FEEDS = [
        "https://api.theregister.com/api/v1/article?orderBy=published&site_id=2&remapper=rss",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime (UTC normalized)."""
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML into readable text:
        - Remove <a>, <script>, <style>, <iframe>, etc.
        - Remove visible URLs (http, https, www)
        - Collapse extra spaces
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style", "iframe", "noscript"]):
                tag.decompose()
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)
            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def extract_image(item):
        """
        The Register's remapper=rss output carries the lead image as an
        <enclosure> and/or <media:thumbnail> tag rather than an <image> tag.
        Prefer enclosure, fall back to media:thumbnail.
        """
        enclosure = item.find("enclosure")
        if enclosure and enclosure.get("url"):
            return enclosure.get("url")

        thumbnail = item.find("media:thumbnail")
        if thumbnail and thumbnail.get("url"):
            return thumbnail.get("url")

        return None

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse The Register's RSS-remapped API feed using cloudscraper."""
        try:
            logger.info(f"Fetching The Register RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(feed_url, timeout=30, headers=get_random_headers())
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
                    category_elem = item.find("category")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    pub_date = (
                        TheRegisterRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    if content_elem:
                        content = TheRegisterRSSPipeline.clean_content(content_elem.get_text())
                    elif desc_elem:
                        content = TheRegisterRSSPipeline.clean_content(desc_elem.get_text())
                    else:
                        content = ""

                    if len(content) < 200:
                        logger.info(f"Skipped article '{title}' due to content length < 200 chars")
                        continue

                    image_url = TheRegisterRSSPipeline.extract_image(item)
                    genre = category_elem.get_text(strip=True) if category_elem else ""

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": "The Register Staff",
                        "language": "en-US",
                        "image": image_url,
                        "source": TheRegisterRSSPipeline.SOURCE,
                        "content": content,
                        "genre": genre,
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process The Register article item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} The Register articles from feed.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch The Register RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in TheRegisterRSSPipeline.RSS_FEEDS:
                articles = TheRegisterRSSPipeline.fetch_rss_feed(feed_url)
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
            logger.error(f"The Register RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }