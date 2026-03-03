import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class TheVergeRSSPipeline:

    SOURCE = "The Verge"
    RSS_FEEDS = [
        "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
    ]

    @staticmethod
    def parse_date(date_str):
        """
        Parse Atom datetime (ISO 8601) and normalize to UTC.
        Example:
        2026-02-16T21:57:56-05:00
        2026-02-17T03:13:04+00:00
        """
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.fromisoformat(date_str.strip())
            if dt.tzinfo:
                return dt.astimezone(timezone.utc)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            logger.warning(f"Unrecognized Atom date format: {date_str}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML:
        - Remove images, scripts, styles, iframes
        - Remove figure + figcaption
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
        """Fetch and parse The Verge Atom RSS feed."""
        try:
            logger.info(f"Fetching The Verge RSS feed: {feed_url}")

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
            entries = soup.find_all("entry")
            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for entry in entries:
                try:
                    title_elem = entry.find("title")
                    link_elem = entry.find("link", rel="alternate")
                    published_elem = entry.find("published")
                    updated_elem = entry.find("updated")
                    summary_elem = entry.find("summary")
                    content_elem = entry.find("content")
                    author_elem = entry.find("author")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get("href", "").strip()
                    date_source = (
                        published_elem.get_text()
                        if published_elem
                        else updated_elem.get_text() if updated_elem else None
                    )

                    pub_date = TheVergeRSSPipeline.parse_date(date_source)

                    author_name = "The Verge"
                    if author_elem and author_elem.find("name"):
                        author_name = author_elem.find("name").get_text(strip=True)

                    categories = [
                        cat.get("term")
                        for cat in entry.find_all("category")
                        if cat.get("term")
                    ]
                    raw_content = ""
                    if content_elem:
                        raw_content = content_elem.get_text()
                    elif summary_elem:
                        raw_content = summary_elem.get_text()

                    content = TheVergeRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        logger.info(
                            f"Skipped article '{title}' (content < 200 chars)"
                        )
                        continue

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author_name,
                        "language": "en-US",
                        "source": TheVergeRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Technology",
                        "media_origin": "international",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process The Verge article entry: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} The Verge articles."
            )
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch The Verge RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run The Verge RSS pipeline and insert into MongoDB."""
        try:
            all_articles = []

            for feed_url in TheVergeRSSPipeline.RSS_FEEDS:
                articles = TheVergeRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles,
                user_email=input_data.get("email") if input_data else None,
            )

            return result

        except Exception as e:
            logger.error(f"The Verge RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
