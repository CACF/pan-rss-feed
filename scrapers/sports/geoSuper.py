from config import SPORTS_TABLE
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


class GeosuperScraper:
    SOURCE = "Geo Super"

    # Maps each section URL to the category/genre it should be tagged with
    SECTION_URLS = {
        "https://www.geosuper.tv/latest-news": "Latest",
        "https://www.geosuper.tv/category/cricket": "Cricket",
        "https://www.geosuper.tv/category/football": "Football",
        "https://www.geosuper.tv/category/hockey": "Hockey",
        "https://www.geosuper.tv/category/tennis": "Tennis",
        "https://www.geosuper.tv/category/mountaineering": "Mountaineering",
        "https://www.geosuper.tv/category/motorsport": "Motorsport",
        "https://www.geosuper.tv/category/mma": "MMA",
        "https://www.geosuper.tv/category/basketball": "Basketball",
        "https://www.geosuper.tv/category/esports": "Esports",
        "https://www.geosuper.tv/category/golf": "Golf",
        "https://www.geosuper.tv/category/athletics": "Athletics",
        "https://www.geosuper.tv/category/squash": "Squash",
        "https://www.geosuper.tv/category/boxing": "Boxing",
        "https://www.geosuper.tv/category/baseball": "Baseball",
        "https://www.geosuper.tv/category/olympics": "Olympics",
    }

    HEADERS = {"Accept-Language": "en-US,en;q=0.9"}

    # Common date formats seen on Geo Super article pages, tried in order
    DATE_FORMATS = [
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y",
        "%d %B, %Y %I:%M %p",
        "%d %B, %Y",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ]

    @classmethod
    def parse_date(cls, date_str: str):
        """Parse Geo Super date strings to datetime, trying known formats."""
        if not date_str:
            return datetime.now(timezone.utc)
        clean_str = date_str.strip().strip("|").strip()
        # Strip common prefixes like "Updated:" / "Published:"
        clean_str = re.sub(
            r"^(updated|published)\s*:?\s*", "", clean_str, flags=re.IGNORECASE
        )

        for fmt in cls.DATE_FORMATS:
            try:
                dt = datetime.strptime(clean_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        try:
            dt = datetime.fromisoformat(clean_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        logger.warning(f"Failed to parse date '{date_str}', defaulting to now")
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
        """Get (url, category) pairs for the latest Geo Super articles across all sections."""
        try:
            links = []
            for section_url, category in cls.SECTION_URLS.items():
                logger.info(f"Fetching article links from: {section_url}")
                try:
                    resp = requests.get(
                        section_url, timeout=30, headers=get_random_headers(cls.HEADERS)
                    )
                    try:
                        resp.raise_for_status()
                        payload = resp.content
                    finally:
                        resp.close()
                except Exception as e:
                    logger.error(f"Failed to fetch section {section_url}: {e}")
                    continue

                soup = BeautifulSoup(payload, "html.parser")
                anchors = soup.select(".heading > h2 > a")
                for a in anchors:
                    href = a.get("href")
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = "https://www.geosuper.tv" + href
                    links.append((href, category))

            # Deduplicate by URL, keeping the first category seen for each link
            seen = {}
            for url, category in links:
                if url not in seen:
                    seen[url] = category

            return list(seen.items())
        except Exception as e:
            logger.error(f"Failed to fetch article links: {e}")
            return []

    @classmethod
    def fetch_article(cls, url, category):
        """Fetch and parse an individual Geo Super article."""
        try:
            resp = requests.get(
                url, timeout=30, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")

            title_elem = soup.select_one(".container .detail_top_head h1")
            title = title_elem.get_text(strip=True) if title_elem else ""

            author_elem = soup.select_one(".detail_top_head .category-source")
            author = author_elem.get_text(strip=True) if author_elem else "Geo Super"

            date_elem = soup.select_one(".detail_top_head .category-date")
            date_str = date_elem.get_text(strip=True) if date_elem else ""
            pub_date = cls.parse_date(date_str)

            paragraphs = soup.select(".detail_page_left > p")
            content_paragraphs = []
            for p in paragraphs:
                text = p.get_text(" ", strip=True)
                if text and not text.lower().startswith("copyright"):
                    content_paragraphs.append(text)

            raw_content = " ".join(content_paragraphs)
            content = cls.clean_content(raw_content)

            if not title or not content:
                logger.debug(f"Skipping article with missing title/content: {url}")
                return None

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
                "genre": "Sports",
                "media_origin": "local",
                "tags": [category],
            }
            return article
        except Exception as e:
            logger.warning(f"Failed to fetch Geo Super article {url}: {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = SPORTS_TABLE
            article_links = GeosuperScraper.fetch_article_links()

            logger.info(f"Found {len(article_links)} article links")

            all_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(
                        GeosuperScraper.fetch_article,
                        url,
                        category,
                    ): url
                    for url, category in article_links
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
                all_articles, table_name=target_table, category="sports"
            )

            return result

        except Exception as e:
            logger.error(f"Geo Super pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
