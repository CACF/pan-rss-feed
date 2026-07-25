from config import BUSINESS_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import requests

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class AIBusinessStrategyRSSPipeline:

    SOURCE = "AI News"

    RSS_FEEDS = [
        "https://www.artificialintelligence-news.com/categories/inside-ai/ai-business-strategy/feed/"
    ]

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.strptime(
                date_str.strip(),
                "%a, %d %b %Y %H:%M:%S %z"
            )
            return dt.astimezone(timezone.utc)

        except Exception:
            logger.warning(f"Invalid date format: {date_str}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""

        soup = BeautifulSoup(content_html, "html.parser")

        for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
            tag.decompose()

        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if "The post" in text or not text:
                p.decompose()

        text = soup.get_text(separator=" ")
        text = re.sub(r"http\S+|www\.\S+", "", text)

        return " ".join(text.split())

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

                    content = AIBusinessStrategyRSSPipeline.clean_content(raw_content)

                    if len(content) < 300:
                        continue

                    author = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else "AI News Staff"
                    )

                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                        if cat.get_text(strip=True)
                    ]

                    articles.append({
                        "id": link,
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
                    })

                except Exception as e:
                    logger.warning(f"Failed to process item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} AI Business articles.")
            return articles

        except Exception as e:
            logger.error(f"AIBusinessStrategy RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in AIBusinessStrategyRSSPipeline.RSS_FEEDS:
                articles = AIBusinessStrategyRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            return SupabaseClient.insert_articles(all_articles, table_name=target_table)

        except Exception as e:
            logger.error(f"AI Business Strategy pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
