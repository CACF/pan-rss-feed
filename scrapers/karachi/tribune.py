from config import KARACHI_TABLE
import uuid
import logging
import time
import concurrent.futures
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class TribuneKarachiRSSPipeline:

    SOURCE = "Tribune"
    RSS_FEEDS = [
        "https://tribune.com.pk/feed/sindh"
    ]

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    # This feed is titled "Sindh News" and has no dedicated Karachi category —
    # it's Karachi-dominated by default, so instead of requiring "karachi" to
    # appear, we exclude stories clearly datelined to other Sindh districts.
    NON_KARACHI_CITIES = [
        "mirpurkhas", "larkana", "hyderabad", "sukkur", "thatta",
        "umerkot", "badin", "nawabshah", "sanghar", "ghotki",
        "jacobabad", "shikarpur", "khairpur", "dadu", "tando",
        "islamkot", "shaheed benazirabad", "kashmore", "matiari",
    ]

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue

        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "aside", "figure", "iframe"]):
                tag.decompose()

            for a in soup.find_all("a"):
                a.unwrap()

            text = soup.get_text(separator=" ", strip=True)
            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def clean_author(author_raw: str) -> str:
        if not author_raw:
            return "Tribune Karachi Desk"

        author = re.sub(r"\b\S+@\S+\b", "", author_raw)
        author = author.replace("(", "").replace(")", "")
        author = " ".join(author.split())

        return author or "Tribune Karachi Desk"

    @staticmethod
    def is_non_karachi(title: str, link: str) -> bool:
        haystack = f"{title} {link}".lower()
        return any(city in haystack for city in TribuneKarachiRSSPipeline.NON_KARACHI_CITIES)

    @staticmethod
    def fetch_tribune_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Tribune Karachi RSS feed: {feed_url}")
            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(TribuneKarachiRSSPipeline.HEADERS),
            )
            try:
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            if not items:
                return []

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    desc_elem = item.find("description")
                    pub_date_elem = item.find("pubDate")
                    creator_elem = item.find("dc:creator")
                    content_encoded_elem = item.find("content:encoded")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                    ]

                    if TribuneKarachiRSSPipeline.is_non_karachi(title, link):
                        logger.info(f"Skipping non-Karachi Sindh story: '{title}' ({link})")
                        continue

                    pub_date = pub_date_elem.get_text(strip=True) if pub_date_elem else ""

                    author_raw = creator_elem.get_text(strip=True) if creator_elem else ""
                    author = TribuneKarachiRSSPipeline.clean_author(author_raw)

                    content_html = (
                        content_encoded_elem.get_text()
                        if content_encoded_elem
                        else (desc_elem.get_text() if desc_elem else "")
                    )

                    content = TribuneKarachiRSSPipeline.clean_content(content_html)

                    if len(content) < 200:
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": TribuneKarachiRSSPipeline.parse_date(pub_date),
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": TribuneKarachiRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": categories,
                    })

                except Exception as e:
                    logger.warning(f"Failed to process Tribune article: {e}")
                    continue

            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Tribune RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        try:
            logger.info("Starting Tribune Karachi RSS pipeline (concurrent)")
            all_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(TribuneKarachiRSSPipeline.fetch_tribune_rss_feed, feed): feed
                    for feed in TribuneKarachiRSSPipeline.RSS_FEEDS
                }

                for future in concurrent.futures.as_completed(futures):
                    try:
                        all_articles.extend(future.result())
                    except Exception:
                        logger.exception("Feed processing failed")

            return all_articles

        except Exception as e:
            logger.error(f"Tribune Karachi RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = TribuneKarachiRSSPipeline.process_input()

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            return SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

        except Exception as e:
            logger.error(f"Tribune Karachi RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }