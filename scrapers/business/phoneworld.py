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


class PhoneWorldRSSPipeline:

    SOURCE = "PhoneWorld"

    RSS_FEEDS = [
        "https://www.phoneworld.com.pk/tag/business/feed/",
    ]

    @staticmethod
    def parse_date(date_str):
        """
        Parse RSS pubDate format:
        Example:
        Wed, 10 May 2023 07:04:42 +0000
        """
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.strptime(
                date_str.strip(),
                "%a, %d %b %Y %H:%M:%S %z"
            )
            return dt.astimezone(timezone.utc)
        except Exception:
            logger.warning(f"Unrecognized RSS date format: {date_str}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean WordPress HTML content:
        - Remove images, scripts, styles, iframes
        - Remove figures, ads divs
        - Remove copyright notice
        - Normalize whitespace
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup([
                "script",
                "style",
                "iframe",
                "noscript",
                "img",
                "figure"
            ]):
                tag.decompose()

            text = soup.get_text(separator=" ")

            # Remove copyright boilerplate
            text = re.sub(
                r"Copyright protected content copied from PhoneWorld website\.",
                "",
                text,
                flags=re.IGNORECASE,
            )

            # Remove URLs
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse PhoneWorld Business RSS feed."""
        try:
            logger.info(f"Fetching PhoneWorld RSS feed: {feed_url}")

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
                    pub_elem = item.find("pubDate")
                    author_elem = item.find("dc:creator")
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = PhoneWorldRSSPipeline.parse_date(
                        pub_elem.get_text() if pub_elem else None
                    )

                    author_name = (
                        author_elem.get_text(strip=True)
                        if author_elem
                        else "PhoneWorld"
                    )

                    raw_content = ""
                    if content_elem:
                        raw_content = content_elem.get_text()
                    elif desc_elem:
                        raw_content = desc_elem.get_text()

                    content = PhoneWorldRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        logger.info(
                            f"Skipped article '{title}' (content < 200 chars)"
                        )
                        continue

                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                        if cat.get_text(strip=True).lower() != "news"
                    ]

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author_name,
                        "language": "en-US",
                        "source": PhoneWorldRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process PhoneWorld article entry: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} PhoneWorld articles."
            )
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch PhoneWorld RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in PhoneWorldRSSPipeline.RSS_FEEDS:
                articles = PhoneWorldRSSPipeline.fetch_rss_feed(feed_url)
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
