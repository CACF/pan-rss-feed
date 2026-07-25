from config import SPORTS_TABLE
import re
import uuid
import logging
import concurrent.futures
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class CBSSportsScraper:
    SOURCE = "CBS Sports"
    RSS_FEED = "https://www.cbssports.com/rss/headlines/soccer/"

    HEADERS = {"Accept-Language": "en-US,en;q=0.9"}

    @staticmethod
    def parse_date(date_str: str):
        try:
            return datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z")
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(text: str):
        if not text:
            return ""
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @classmethod
    def fetch_rss(cls):
        try:
            resp = requests.get(
                cls.RSS_FEED,
                timeout=30,
                headers=get_random_headers(cls.HEADERS),
            )
            resp.raise_for_status()

            soup = BeautifulSoup(resp.content, "xml")

            items = []

            for item in soup.find_all("item"):
                title = item.find("title").text.strip() if item.find("title") else ""
                link = item.find("link").text.strip() if item.find("link") else ""
                pub_date = (
                    item.find("pubDate").text.strip() if item.find("pubDate") else ""
                )
                creator = item.find("dc:creator")
                author = creator.text.strip() if creator else "Unknown"

                enclosure = item.find("enclosure")
                image = (
                    enclosure["url"]
                    if enclosure and enclosure.has_attr("url")
                    else None
                )

                items.append(
                    {
                        "title": title,
                        "link": link,
                        "pub_date": pub_date,
                        "author": author,
                        "image": image,
                    }
                )

            return items

        except Exception as e:
            logger.error(f"RSS fetch failed: {e}")
            return []

    @classmethod
    def fetch_article_content(cls, url: str):
        """
        Extract full article content using CSS selector ONLY:
        article#Article-body .Article-bodyContent p
        """
        try:
            resp = requests.get(
                url,
                timeout=30,
                headers=get_random_headers(cls.HEADERS),
            )
            resp.raise_for_status()

            soup = BeautifulSoup(resp.content, "html.parser")

            paragraphs = soup.select("article#Article-body .Article-bodyContent p")

            content = " ".join(
                p.get_text(" ", strip=True)
                for p in paragraphs
                if p.get_text(strip=True)
            )

            return cls.clean_content(content)

        except Exception as e:
            logger.warning(f"Content extraction failed: {url} -> {e}")
            return ""

    @classmethod
    def fetch_article(cls, item: dict):
        try:
            url = item["link"]

            content = cls.fetch_article_content(url)

            return {
                "id": url,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": cls.parse_date(item["pub_date"]),
                "feedBuildDate": datetime.now(timezone.utc),
                "title": item["title"],
                "authors": item["author"],
                "image": item["image"],
                "language": "en-us",
                "source": cls.SOURCE,
                "content": content,
                "genre": "Sports",
                "media_origin": "International",
                "tags": [],
            }

        except Exception as e:
            logger.warning(f"Article failed: {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or SPORTS_TABLE
            items = CBSSportsScraper.fetch_rss()

            logger.info(f"RSS items found: {len(items)}")

            articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(CBSSportsScraper.fetch_article, item)
                    for item in items
                ]

                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res:
                        articles.append(res)

            # Deduplicate
            articles = list({a["id"]: a for a in articles}.values())

            logger.info(f"Final articles after dedupe: {len(articles)}")

            return SupabaseClient.insert_articles(articles, table_name=target_table)

        except Exception as e:
            logger.error(f"CBS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
