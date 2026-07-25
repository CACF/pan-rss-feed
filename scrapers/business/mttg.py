from config import BUSINESS_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import concurrent.futures
import requests

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class MettisglobalBusinessScraper:
    SOURCE = "Mettis Global"
    SECTION_URLS = [
        "https://mettisglobal.news/latest/",
        "https://mettisglobal.news/equity/",
        "https://mettisglobal.news/forex/",
    ]

    HEADERS = {"Accept-Language": "en-US,en;q=0.9"}

    @staticmethod
    def parse_date(date_str: str):
        """Parse Mettis Global or ISO 8601 date strings to datetime."""
        try:
            clean_str = date_str.lstrip("| ").strip()
            try:
                dt = datetime.fromisoformat(clean_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                dt = datetime.strptime(clean_str, "%B %d, %Y at %I:%M %p GMT%z")
                return dt
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(html):
        """
        Clean article HTML:
        - Remove scripts, styles, and asides
        - Unwrap <a> tags (keep text, remove links)
        - Remove visible URLs (http, https, www)
        - Normalize spaces
        """
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "aside"]):
                tag.decompose()
            for a in soup.find_all("a"):
                a.unwrap()
            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"clean_content error: {e}")
            return html

    @classmethod
    def fetch_article_links(cls):
        """Get links to latest Mettis Global articles."""
        try:
            links = []
            for section_url in cls.SECTION_URLS:
                logger.info(f"Fetching article links from: {section_url}")
                resp = requests.get(section_url, timeout=30, headers=get_random_headers(cls.HEADERS))
                try:
                    resp.raise_for_status()
                    payload = resp.content
                finally:
                    resp.close()
                soup = BeautifulSoup(payload, "html.parser")

                headings = soup.select("div#Posts div.post.PostList a")
                for a in headings:
                    href = a.get("href")
                    if href:
                        links.append(href)

            return list(set(links))
        except Exception as e:
            logger.error(f"Failed to fetch article links: {e}")
            return []

    def format_iso8601(dt: datetime) -> str:
        iso_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        offset = dt.strftime("%z")
        return iso_date + offset[:-2] + ":" + offset[-2:]

    @classmethod
    def fetch_article(cls, url):
        """Fetch and parse an individual Mettis Global article."""
        try:
            resp = requests.get(url, timeout=30, headers=get_random_headers(cls.HEADERS))
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")
            title_elem = soup.select_one("div.postNews h1")
            title = title_elem.get_text(strip=True) if title_elem else ""
            time_elem = soup.select_one("div.postNews span.ListnewsDate")
            date_str = (
                time_elem.get("datetime")
                if time_elem and time_elem.has_attr("datetime")
                else (time_elem.get_text(strip=True) if time_elem else "")
            )
            pub_date = cls.parse_date(date_str) if date_str else datetime.now(timezone.utc)

            author_elem = soup.select_one("div.postNews p.Listnewscategroy a")
            author = author_elem.get_text(strip=True) if author_elem else "Unknown"
            paragraphs = soup.select("div#text p")
            content_paragraphs = []
            for p in paragraphs:
                text = p.get_text(" ", strip=True)
                if text and not text.lower().startswith("copyright"):
                    content_paragraphs.append(text)

            raw_content = " ".join(content_paragraphs)
            content = cls.clean_content(raw_content)
           
            build_date = datetime.now(timezone.utc)
            article = {
                "id": url,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": build_date,
                "title": title,
                "authors": author,
                "language": "en-us",
                "source": cls.SOURCE,
                "content": content,
                "genre": "Business",
                "media_origin": "local",
                "tags": [],
            }
            return article
        except Exception as e:
            logger.warning(f"Failed to fetch Mettis Global article {url}: {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            article_links = MettisglobalBusinessScraper.fetch_article_links()

            logger.info(
                f"Found {len(article_links)} article links"
            )

            all_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(
                        MettisglobalBusinessScraper.fetch_article,
                        url,
                    ): url
                    for url in article_links
                }

                for future in concurrent.futures.as_completed(futures):
                    article = future.result()
                    if article:
                        all_articles.append(article)

            # Deduplicate by URL
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"Mettis Global pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
