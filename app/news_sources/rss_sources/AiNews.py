import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import requests

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class AIBusinessStrategyRSSPipeline:
    """
    AI News – AI Business Strategy RSS pipeline
    WordPress RSS 2.0 feed
    Production-safe, Flask-compatible
    """

    SOURCE = "AI News"

    RSS_FEEDS = [
        "https://www.artificialintelligence-news.com/categories/inside-ai/ai-business-strategy/feed/"
    ]

    # --------------------------------------------------
    # DATE PARSER
    # --------------------------------------------------
    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception:
            logger.warning(f"Invalid date format: {date_str}")
            return datetime.now(timezone.utc)

    # --------------------------------------------------
    # CONTENT CLEANER
    # --------------------------------------------------
    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""

        soup = BeautifulSoup(content_html, "html.parser")

        # Remove unwanted tags
        for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
            tag.decompose()

        # Remove “The post appeared first on...” footer
        for p in soup.find_all("p"):
            if "The post" in p.get_text():
                p.decompose()

        # Remove empty paragraphs
        for p in soup.find_all("p"):
            if not p.get_text(strip=True):
                p.decompose()

        text = soup.get_text(separator=" ")
        text = re.sub(r"http\S+|www\.\S+", "", text)

        return " ".join(text.split())

    # --------------------------------------------------
    # FETCH RSS
    # --------------------------------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching AI Business Strategy RSS: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(),
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "lxml-xml")
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
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        AIBusinessStrategyRSSPipeline.parse_date(
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

                    content = AIBusinessStrategyRSSPipeline.clean_content(
                        raw_content
                    )

                    if len(content) < 300:
                        logger.info(f"Skipped short article: {title}")
                        continue

                    author = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else "AI News Staff"
                    )

                    # Extract categories as tags
                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                    ]

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en",
                        "source": AIBusinessStrategyRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "AI Business",
                        "media_origin": "foreign",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} AI Business articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return []

    # --------------------------------------------------
    # RUN PIPELINE
    # --------------------------------------------------
    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in AIBusinessStrategyRSSPipeline.RSS_FEEDS:
                articles = AIBusinessStrategyRSSPipeline.fetch_rss_feed(
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
            logger.error(f"AI Business Strategy RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
