import re
import uuid
import logging
from datetime import datetime, timezone

import cloudscraper
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class TennisAbstractRSSPipeline:

    SOURCE = "TennisAbstract"

    RSS_FEEDS = [
        "https://www.tennisabstract.com/blog/feed/",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%SZ",
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
    def clean_content(content_html):
        """Clean HTML and remove unwanted elements/URLs."""
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

            text = re.sub(
                r"The post .*? appeared first on .*?\.",
                "",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse Tennis Abstract RSS feed."""
        try:
            logger.info(f"Fetching Tennis Abstract RSS feed: {feed_url}")

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

            channel_image = None
            image_tag = soup.find("image")
            if image_tag:
                url_tag = image_tag.find("url")
                if url_tag:
                    channel_image = url_tag.get_text(strip=True)

            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")
                    creator_elem = item.find("dc:creator")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        TennisAbstractRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    if content_elem:
                        content = TennisAbstractRSSPipeline.clean_content(
                            content_elem.get_text()
                        )
                    elif desc_elem:
                        content = TennisAbstractRSSPipeline.clean_content(
                            desc_elem.get_text()
                        )
                    else:
                        content = ""

                    if len(content) < 200:
                        logger.info(
                            f"Skipped article '{title}' due to content length < 200 chars"
                        )
                        continue

                    tags = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                    ]

                    author = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else "Tennis Abstract"
                    )

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "image": channel_image,
                        "source": TennisAbstractRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Sports",
                        "media_origin": "local",
                        "tags": tags,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process Tennis Abstract article item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} Tennis Abstract articles from feed."
            )

            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Tennis Abstract RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in TennisAbstractRSSPipeline.RSS_FEEDS:
                articles = TennisAbstractRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"Tennis Abstract RSS pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }