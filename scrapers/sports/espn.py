import re
import uuid
import logging
import concurrent.futures
from datetime import datetime, timezone
from config import SPORTS_TABLE

import requests
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ESPNScraper:
    SOURCE = "ESPN"

    SECTION_URLS = [
        "https://www.espn.in/football/",
        "https://www.espn.in/cricket/",
    ]

    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
    }

    @staticmethod
    def clean_content(text):
        if not text:
            return ""

        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%b %d, %Y, %I:%M %p",
            "%B %d, %Y, %I:%M %p",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass

        return datetime.now(timezone.utc)

    @classmethod
    def fetch_article_links(cls):
        """
        Fetch article links from ESPN category pages.
        """

        links = set()

        try:
            for section_url in cls.SECTION_URLS:

                logger.info(f"Fetching links from: {section_url}")

                response = requests.get(
                    section_url,
                    timeout=30,
                    headers=get_random_headers(cls.HEADERS),
                )

                try:
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, "html.parser")

                    for a in soup.select('a[href*="/story/_/id/"]'):
                        href = a.get("href")

                        if not href:
                            continue

                        if href.startswith("/"):
                            href = "https://www.espn.in" + href

                        if "/story/_/id/" in href:
                            links.add(href)

                finally:
                    response.close()

        except Exception as e:
            logger.error(f"Failed fetching ESPN links: {e}")

        return list(links)

    @classmethod
    def fetch_article(cls, url):
        try:
            response = requests.get(
                url,
                timeout=30,
                headers=get_random_headers(cls.HEADERS),
            )

            try:
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

            finally:
                response.close()

            title_elem = soup.select_one("article.article header.article-header h1")

            title = title_elem.get_text(strip=True) if title_elem else ""

            author_elem = soup.select_one("div.article-meta div.author")

            author = author_elem.get_text(strip=True) if author_elem else "Unknown"

            date_elem = soup.select_one("div.article-meta span.timestamp")

            date_str = date_elem.get_text(strip=True) if date_elem else ""

            pub_date = cls.parse_date(date_str)

            content_container = soup.select_one("article.article div.article-body")

            content = ""

            if content_container:

                for tag in content_container.select("""
                    aside,
                    script,
                    style,
                    iframe,
                    .content-reactions,
                    .editorial,
                    .teads-adCall,
                    .ad-slot,
                    .sponsored-links
                    """):
                    tag.decompose()

                paragraphs = []

                for p in content_container.select("p"):
                    txt = p.get_text(" ", strip=True)

                    if txt:
                        paragraphs.append(txt)

                content = cls.clean_content(" ".join(paragraphs))

            if not title or not content:
                return None

            build_date = datetime.now(timezone.utc)

            return {
                "id": url,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": build_date,
                "title": title,
                "authors": author,
                "language": "en-us",
                "source": cls.SOURCE,
                "content": content,
                "genre": "Sports",
                "media_origin": "international",
                "tags": [],
            }

        except Exception as e:
            logger.warning(f"Failed article: {url} | {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = table_name or SPORTS_TABLE

        try:

            article_links = ESPNScraper.fetch_article_links()

            logger.info(f"Found {len(article_links)} article links")

            all_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

                futures = {
                    executor.submit(
                        ESPNScraper.fetch_article,
                        url,
                    ): url
                    for url in article_links
                }

                for future in concurrent.futures.as_completed(futures):
                    article = future.result()

                    if article:
                        all_articles.append(article)

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table
            )

            return result

        except Exception as e:
            logger.error(f"ESPN pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
