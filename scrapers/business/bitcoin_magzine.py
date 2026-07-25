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


class BitcoinMagazineRSSPipeline:

    SOURCE = "Bitcoin Magazine"
    RSS_FEEDS = [
        "https://bitcoinmagazine.com/feed",
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
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_author(author_text):
        return author_text.strip() if author_text else "Bitcoin Magazine Staff"

    @staticmethod
    def clean_content(content_html):
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
            logger.warning(f"Content cleaning failed: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Bitcoin Magazine RSS: {feed_url}")

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
                    author_elem = item.find("dc:creator")

                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        BitcoinMagazineRSSPipeline.parse_date(pub_date_elem.get_text())
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

                    content = BitcoinMagazineRSSPipeline.clean_content(raw_content)

                    if len(content) < 300:
                        continue

                    author = (
                        BitcoinMagazineRSSPipeline.clean_author(
                            author_elem.get_text(strip=True)
                        )
                        if author_elem
                        else "Bitcoin Magazine Staff"
                    )

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "source": BitcoinMagazineRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Crypto / Bitcoin",
                        "media_origin": "international",
                        "tags": [],
                    })

                except Exception as e:
                    logger.warning(f"Failed to process Bitcoin Magazine item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Bitcoin Magazine articles.")
            return articles

        except Exception as e:
            logger.error(f"Bitcoin Magazine RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in BitcoinMagazineRSSPipeline.RSS_FEEDS:
                articles = BitcoinMagazineRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            return SupabaseClient.insert_articles(all_articles, table_name=target_table)

        except Exception as e:
            logger.error(f"Bitcoin Magazine RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
