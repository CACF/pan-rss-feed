from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import logging
import re
from urllib.parse import urljoin
import uuid
from bs4 import BeautifulSoup
import cloudscraper
import requests

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient
from config import KARACHI_TABLE

logger = logging.getLogger(__name__)


class SamaaNewsKarachiPipeline:

    SOURCE = "SAMAA TV"
    BASE_URL = "https://www.samaa.tv"
    LISTING_URL = "https://www.samaa.tv/trends/karachi-news"

    # Parallel workers for fetching article contents
    MAX_WORKERS = 8

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        date_str = date_str.strip()

        formats = [
            "%B %d, %Y",               # September 19, 2026
            "%b %d, %Y",               # Sep 19, 2026
            "%a, %d %b %Y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def fetch_article_details(article_url):
        """
        Visits the article page and extracts:
        - title:   h1.entry-title
        - author:  .author-name
        - date:    .entry-meta .me-10
        - content: .entry-content-html section
        """
        try:
            logger.info(f"Fetching SAMAA article: {article_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    article_url,
                    timeout=25,
                    headers=get_random_headers(SamaaNewsKarachiPipeline.HEADERS),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "html.parser")

            # 1. Title
            title_elem = soup.select_one("h1.entry-title")
            title = title_elem.get_text(strip=True) if title_elem else ""

            # 2. Author
            author_elem = soup.select_one(".author-name")
            author = author_elem.get_text(strip=True) if author_elem else "Samaa Web Desk"

            # 3. Datetime
            date_elem = soup.select_one(".entry-meta .me-10") or soup.select_one(".entry-meta time")
            date_str = date_elem.get_text(strip=True) if date_elem else ""
            pub_date = SamaaNewsKarachiPipeline.parse_date(date_str)

            # 4. Content
            content_section = (
                soup.select_one(".entry-content-html section") 
                or soup.select_one(".entry-content-html")
            )

            if not content_section:
                return None

            # Remove unwanted tags
            for tag in content_section(["script", "style", "iframe", "figure"]):
                tag.decompose()

            for a in content_section.find_all("a"):
                a.unwrap()

            text = content_section.get_text(separator="\n\n", strip=True)
            text = re.sub(r"http\S+|www\.\S+", "", text)
            clean_content = "\n\n".join(
                line.strip() for line in text.splitlines() if line.strip()
            )

            if len(clean_content) < 150:
                logger.info(f"Skipped article '{title}' (content length < 150 chars)")
                return None

            return {
                "title": title,
                "authors": author,
                "articlePubDate": pub_date,
                "content": clean_content,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch SAMAA article {article_url}: {e}")
            return None

    @staticmethod
    def get_article_links():
        """
        Fetches the Karachi news trend page and extracts article URLs
        using selector: .loop-grid-3 .post-title a
        """
        try:
            logger.info(f"Fetching SAMAA Karachi news listing: {SamaaNewsKarachiPipeline.LISTING_URL}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    SamaaNewsKarachiPipeline.LISTING_URL,
                    timeout=30,
                    headers=get_random_headers(SamaaNewsKarachiPipeline.HEADERS),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "html.parser")

            # Extract headline links
            link_elements = soup.select(".loop-grid-3 .post-title a")
            urls = []
            seen = set()

            for a in link_elements:
                href = a.get("href")
                if not href:
                    continue

                full_url = urljoin(SamaaNewsKarachiPipeline.BASE_URL, href)
                if full_url not in seen:
                    seen.add(full_url)
                    urls.append(full_url)

            logger.info(f"Discovered {len(urls)} Karachi article links on SAMAA TV.")
            return urls

        except Exception as e:
            logger.error(f"Failed to fetch SAMAA Karachi listing: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        article_links = SamaaNewsKarachiPipeline.get_article_links()
        if not article_links:
            return []

        articles = []
        feed_build_date = datetime.now(timezone.utc)

        # Fetch articles concurrently
        with ThreadPoolExecutor(max_workers=SamaaNewsKarachiPipeline.MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(SamaaNewsKarachiPipeline.fetch_article_details, url): url
                for url in article_links
            }

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    details = future.result()
                    if not details or not details.get("title"):
                        continue

                    articles.append({
                        "id": url,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": details["articlePubDate"],
                        "feedBuildDate": feed_build_date,
                        "title": details["title"],
                        "authors": details["authors"],
                        "language": "en-US",
                        "source": SamaaNewsKarachiPipeline.SOURCE,
                        "content": details["content"],
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": ["Karachi", "Pakistan"],
                    })

                except Exception as e:
                    logger.warning(f"Error resolving article {url}: {e}")
                    continue

        logger.info(f"Successfully processed {len(articles)} SAMAA Karachi articles.")
        return articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = SamaaNewsKarachiPipeline.process_input()

            # Deduplicate by URL
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            # Insert into Supabase
            return SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

        except Exception as e:
            logger.error(f"SAMAA TV Karachi pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }