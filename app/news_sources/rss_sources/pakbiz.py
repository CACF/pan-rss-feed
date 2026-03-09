import re
import uuid
import logging
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class PakbizRSSPipeline:

    SOURCE = "Pakbiz"

    RSS_FEEDS = [
        "https://pakbiz.com/feed/",
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
                if dt.tzinfo:
                    return dt.astimezone(timezone.utc)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML content
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(
                ["script", "style", "iframe", "noscript", "img", "figure", "table"]
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
        """Fetch Pakbiz RSS feed."""

        try:
            logger.info(f"Fetching Pakbiz RSS: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    headers=get_random_headers(),
                    timeout=30,
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
                    creator_elem = item.find("dc:creator")
                    content_elem = item.find("content:encoded")
                    description_elem = item.find("description")
                    category_elems = item.find_all("category")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

                    pub_date = PakbizRSSPipeline.parse_date(
                        pub_date_elem.get_text() if pub_date_elem else None
                    )

                    if pub_date < seven_days_ago:
                        logger.info(f"Skipped '{title}' (older than 7 days)")
                        continue
                    author = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else "Pakbiz"
                    )

                    categories = [
                        cat.get_text(strip=True)
                        for cat in category_elems
                        if cat.get_text(strip=True)
                    ]

                    raw_content = ""

                    if content_elem:
                        raw_content = content_elem.get_text()
                    elif description_elem:
                        raw_content = description_elem.get_text()

                    content = PakbizRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        logger.info(f"Skipped '{title}' (content too short)")
                        continue

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "source": PakbizRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "pakistan",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed processing Pakbiz item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Pakbiz articles")

            return articles

        except Exception as e:
            logger.error(f"Pakbiz RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run Pakbiz RSS pipeline"""

        try:
            all_articles = []

            for feed_url in PakbizRSSPipeline.RSS_FEEDS:
                articles = PakbizRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles,
                user_email=input_data.get("email") if input_data else None,
            )

            return result

        except Exception as e:
            logger.error(f"Pakbiz pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }