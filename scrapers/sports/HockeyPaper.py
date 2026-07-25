import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import concurrent.futures
import cloudscraper

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class HockeyPaperScraper:
    SOURCE = "The Hockey Paper"
    LISTING_URL = "https://www.thehockeypaper.co.uk/articles/category/latest-news"

    HEADERS = {"Accept-Language": "en-US,en;q=0.9"}
    SCRAPER = cloudscraper.create_scraper()

    @staticmethod
    def parse_date(date_str: str):
        """Parse Hockey Paper (WordPress <time datetime=...>) or ISO 8601 date strings to datetime."""
        try:
            clean_str = date_str.strip()
            dt = datetime.fromisoformat(clean_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
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
        """Get links to latest Hockey Paper articles."""
        try:
            logger.info(f"Fetching article links from: {cls.LISTING_URL}")
            resp = cls.SCRAPER.get(
                cls.LISTING_URL, timeout=30, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()
            soup = BeautifulSoup(payload, "html.parser")

            headings = soup.select(
                ".tdb_module_loop.td_module_wrap h3.entry-title.td-module-title a, h3.entry-title a, .td-module-title a, article a[href*='/articles/'], a[href*='/articles/20']"
            )
            links = []
            for a in headings:
                href = a.get("href")
                if href and "/articles/" in href:
                    links.append(href)

            return list(set(links))
        except Exception as e:
            logger.error(f"Failed to fetch article links: {e}")
            return []

    def format_iso8601(dt: datetime) -> str:
        iso_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        offset = dt.strftime("%z")
        return iso_date + offset[:-2] + ":" + offset[-2:]

    @staticmethod
    def fetch_full_description(soup):
        """Standalone content extractor for a Hockey Paper article page."""
        paragraphs = soup.select(
            "div.tdb-block-inner.td-fix-index p.wp-block-paragraph, div.tdb-block-inner p, article p"
        )
        texts = [p.get_text(" ", strip=True) for p in paragraphs]
        texts = [t for t in texts if t]
        return " ".join(texts)

    @classmethod
    def fetch_article(cls, url):
        """Fetch and parse an individual Hockey Paper article."""
        try:
            resp = cls.SCRAPER.get(
                url, timeout=15, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")
            title_elem = soup.select_one("h1.entry-title") or soup.select_one("h1")
            title = title_elem.get_text(strip=True) if title_elem else ""

            time_elem = soup.select_one(
                "div.tdb-block-inner > time.entry-date"
            ) or soup.select_one("time[datetime]")
            date_str = (
                time_elem.get("datetime")
                if time_elem and time_elem.has_attr("datetime")
                else (time_elem.get_text(strip=True) if time_elem else "")
            )
            pub_date = (
                cls.parse_date(date_str) if date_str else datetime.now(timezone.utc)
            )

            author_elem = soup.select_one("a.tdb-author-name") or soup.select_one(
                ".author-name"
            )
            author = author_elem.get_text(strip=True) if author_elem else "Unknown"

            raw_content = cls.fetch_full_description(soup)
            content = cls.clean_content(raw_content)

            build_date = datetime.now(timezone.utc)
            article = {
                "id": url,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": build_date,
                "title": title,
                "authors": author,
                "language": "en-gb",
                "source": cls.SOURCE,
                "content": content,
                "genre": "Hockey",
                "media_origin": "international",
                "tags": [],
            }
            return article
        except Exception as e:
            logger.warning(f"Failed to fetch Hockey Paper article {url}: {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = table_name or SPORTS_TABLE
        from config import SPORTS_TABLE

        try:
            article_links = HockeyPaperScraper.fetch_article_links()

            logger.info(f"Found {len(article_links)} article links")

            all_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(
                        HockeyPaperScraper.fetch_article,
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

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table
            )

            return result

        except Exception as e:
            logger.error(f"Hockey Paper pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
