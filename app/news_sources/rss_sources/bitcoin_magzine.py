import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class BitcoinMagazineRSSPipeline:

    SOURCE = "Bitcoin Magazine"
    RSS_FEEDS = [
        "https://bitcoinmagazine.com/feed",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to UTC datetime."""
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
        """Normalize dc:creator author."""
        if not author_text:
            return "Bitcoin Magazine Staff"

        return author_text.strip()

    @staticmethod
    def clean_content(content_html):
        """
        Clean WordPress HTML:
        - Remove images, figures, scripts
        - Strip links
        - Normalize whitespace
        """
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
        """Fetch and parse Bitcoin Magazine RSS feed."""
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
                        BitcoinMagazineRSSPipeline.parse_date(
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

                    content = BitcoinMagazineRSSPipeline.clean_content(
                        raw_content
                    )

                    if len(content) < 300:
                        logger.info(
                            f"Skipped '{title}' (content too short)"
                        )
                        continue

                    author = (
                        BitcoinMagazineRSSPipeline.clean_author(
                            author_elem.get_text(strip=True)
                        )
                        if author_elem
                        else "Bitcoin Magazine Staff"
                    )

                    article = {
                        "_id": link,
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
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process Bitcoin Magazine item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} Bitcoin Magazine articles."
            )
            return articles

        except Exception as e:
            logger.error(f"RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run pipeline and insert into MongoDB."""
        try:
            all_articles = []

            for feed_url in BitcoinMagazineRSSPipeline.RSS_FEEDS:
                articles = BitcoinMagazineRSSPipeline.fetch_rss_feed(
                    feed_url
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
            logger.error(f"Bitcoin Magazine pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
