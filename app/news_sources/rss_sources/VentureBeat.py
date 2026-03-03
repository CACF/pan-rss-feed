import re
import uuid
import logging
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class VentureBeatBusinessRSSPipeline:

    SOURCE = "VentureBeat"

    RSS_FEEDS = [
        "https://venturebeat.com/category/business/feed",
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
    def clean_author(author_text: str) -> str:
        if not author_text:
            return "VentureBeat Staff"

        match = re.search(r"\((.*?)\)", author_text)
        if match:
            return match.group(1).strip()

        author_text = re.sub(r"\S+@\S+", "", author_text)
        return author_text.strip() or "VentureBeat Staff"

    @staticmethod
    def clean_content(content_html):
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
    def extract_categories(item):
        categories = item.find_all("category")
        return [cat.get_text(strip=True) for cat in categories if cat]

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching VentureBeat RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
                response.raise_for_status()
                payload = response.content

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    author_elem = item.find("author")

                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    pub_date = (
                        VentureBeatBusinessRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    if pub_date < seven_days_ago:
                        continue

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else desc_elem.get_text()
                        if desc_elem
                        else ""
                    )

                    content = VentureBeatBusinessRSSPipeline.clean_content(raw_content)

                    if len(content) < 200:
                        continue

                    article = {
                        "_id": link_elem.get_text(strip=True),
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title_elem.get_text(strip=True),
                        "authors": VentureBeatBusinessRSSPipeline.clean_author(
                            author_elem.get_text(strip=True) if author_elem else ""
                        ),
                        "language": "en-US",
                        "source": VentureBeatBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "international",
                        "tags": VentureBeatBusinessRSSPipeline.extract_categories(item),
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process article: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} recent VentureBeat articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in VentureBeatBusinessRSSPipeline.RSS_FEEDS:
                articles = VentureBeatBusinessRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return MongoDBClient.insert_articles_to_mongo(
                all_articles,
                user_email=input_data.get("email") if input_data else None,
            )

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
