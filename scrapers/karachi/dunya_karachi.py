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


class DunyaNewsKarachiPipeline:

    SOURCE = "DunyaNews"
    BASE_URL = "https://dunyanews.tv"
    LISTING_URL = "https://www.dunyanews.tv/en/tags/Karachi/73"

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
    def parse_date(date_str: str) -> datetime:
        """Parses dates such as '23 Sep 26, 17:05:44 PKT' or '2026-09-23' into UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        clean_str = date_str.replace("PKT", "").strip()

        formats = [
            "%d %b %y, %H:%M:%S",   # 23 Sep 26, 17:05:44
            "%d %b %Y, %H:%M:%S",   # 23 Sep 2026, 17:05:44
            "%Y-%m-%d",             # 2026-09-23
            "%B %d, %Y",            # September 23, 2026
            "%b %d, %Y",            # Sep 23, 2026
            "%d-%m-%Y",             # 23-09-2026
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(clean_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: '{date_str}', falling back to now(UTC)")
        return datetime.now(timezone.utc)

    @staticmethod
    def fetch_article_details(article_url: str):
        """
        Visits the article page and extracts:
        - title:   h1.article__heading
        - author:  "Dunya News"
        - date:    .article__data__publish__date time
        - content: .news_html_desc
        """
        try:
            logger.info(f"Fetching Dunya News article: {article_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    article_url,
                    timeout=25,
                    headers=get_random_headers(DunyaNewsKarachiPipeline.HEADERS),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "html.parser")

            # 1. Title
            title_elem = soup.select_one("h1.article__heading")
            title = title_elem.get_text(strip=True) if title_elem else ""

            if not title:
                logger.warning(f"No title found for {article_url}")
                return None

            # 2. Author (website name)
            author = "Dunya News"

            # 3. Datetime
            time_elem = soup.select_one(".article__data__publish__date time")
            date_str = ""
            if time_elem:
                date_str = time_elem.get_text(strip=True) or time_elem.get("datetime", "")
            pub_date = DunyaNewsKarachiPipeline.parse_date(date_str)

            # 4. Content
            content_div = (
                soup.select_one(".news_html_desc")
                or soup.select_one("#speechcontentDiv .news_html_desc")
            )

            if not content_div:
                logger.warning(f"No content found for {article_url}")
                return None

            # Remove unwanted tags
            for tag in content_div(["script", "style", "iframe", "figure", "noscript"]):
                tag.decompose()

            for a in content_div.find_all("a"):
                a.unwrap()

            text = content_div.get_text(separator="\n\n", strip=True)
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
            logger.warning(f"Failed to fetch Dunya News article {article_url}: {e}")
            return None

    @staticmethod
    def get_article_links():
        """
        Fetches the Karachi tag page and extracts article URLs
        using selector: .smalltagnews__title a
        """
        try:
            logger.info(f"Fetching Dunya News Karachi listing: {DunyaNewsKarachiPipeline.LISTING_URL}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    DunyaNewsKarachiPipeline.LISTING_URL,
                    timeout=30,
                    headers=get_random_headers(DunyaNewsKarachiPipeline.HEADERS),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "html.parser")

            # Extract headline links
            link_elements = soup.select(".smalltagnews__title a")
            urls = []
            seen = set()

            for a in link_elements:
                href = a.get("href")
                if not href:
                    continue

                full_url = urljoin(DunyaNewsKarachiPipeline.BASE_URL, href)
                if full_url not in seen:
                    seen.add(full_url)
                    urls.append(full_url)

            logger.info(f"Discovered {len(urls)} Karachi article links on Dunya News.")
            return urls

        except Exception as e:
            logger.error(f"Failed to fetch Dunya News Karachi listing: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        article_links = DunyaNewsKarachiPipeline.get_article_links()
        if not article_links:
            return []

        articles = []
        feed_build_date = datetime.now(timezone.utc)

        # Concurrently fetch article contents
        with ThreadPoolExecutor(max_workers=DunyaNewsKarachiPipeline.MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(DunyaNewsKarachiPipeline.fetch_article_details, url): url
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
                        "source": DunyaNewsKarachiPipeline.SOURCE,
                        "content": details["content"],
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": ["Karachi", "Pakistan"],
                    })

                except Exception as e:
                    logger.warning(f"Error resolving article {url}: {e}")
                    continue

        logger.info(f"Successfully processed {len(articles)} Dunya News Karachi articles.")
        return articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = DunyaNewsKarachiPipeline.process_input()

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
            logger.error(f"Dunya News Karachi pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }