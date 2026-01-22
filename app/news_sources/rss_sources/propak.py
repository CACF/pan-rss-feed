import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class ProPakistaniBusinessRSSPipeline:
    """
    ProPakistani Business RSS feed pipeline
    Flask-compatible, single-threaded, production-safe
    """

    SOURCE = "ProPakistani"
    RSS_FEEDS = [
        "https://propakistani.pk/category/business/feed/",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime (UTC normalized)."""
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
    def clean_content(content_html):
        """
        Clean WordPress HTML into readable text:
        - Prefer content:encoded
        - Remove scripts, styles, images, lead forms
        - Remove WP boilerplate
        - Normalize whitespace
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(
                [
                    "script",
                    "style",
                    "iframe",
                    "noscript",
                    "img",
                    "figure",
                    "form",
                    "span",
                ]
            ):
                tag.decompose()

            text = soup.get_text(separator=" ")

            # Remove WP boilerplate
            text = re.sub(
                r"The post .*? appeared first on .*?\.",
                "",
                text,
                flags=re.IGNORECASE,
            )

            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse ProPakistani Business RSS feed."""
        try:
            logger.info(f"Fetching ProPakistani RSS feed: {feed_url}")

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
                    category_elems = item.find_all("category")

                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        ProPakistaniBusinessRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
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

                    content = (
                        ProPakistaniBusinessRSSPipeline.clean_content(
                            raw_content
                        )
                    )

                    if len(content) < 200:
                        logger.info(
                            f"Skipped article '{title}' (content < 200 chars)"
                        )
                        continue

                    authors = (
                        author_elem.get_text(strip=True)
                        if author_elem
                        else "ProPakistani Business Desk"
                    )

                    tags = [
                        cat.get_text(strip=True)
                        for cat in category_elems
                        if cat.get_text(strip=True)
                    ]

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": authors,
                        "language": "en-US",
                        "source": ProPakistaniBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": tags,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process ProPakistani item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} ProPakistani business articles."
            )
            return articles

        except Exception as e:
            logger.error(
                f"Failed to fetch ProPakistani RSS feed: {e}"
            )
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run ProPakistani Business RSS pipeline and insert into MongoDB."""
        try:
            all_articles = []

            for feed_url in (
                ProPakistaniBusinessRSSPipeline.RSS_FEEDS
            ):
                articles = (
                    ProPakistaniBusinessRSSPipeline.fetch_rss_feed(
                        feed_url
                    )
                )
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles,
                user_email=input_data.get("email") if input_data else None,
            )
            return result

        except Exception as e:
            logger.error(
                f"ProPakistani RSS pipeline failed: {e}"
            )
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
