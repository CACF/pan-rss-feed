from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import logging
import re
from urllib.parse import urljoin
import urllib3
import uuid
from bs4 import BeautifulSoup
import requests

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient
from config import KARACHI_TABLE

# Disable SSL verification warnings for government domains
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class KMCPipeline:

    SOURCE = "KMC"
    WEBSITE_NAME = "Karachi Metropolitan Corporation"
    BASE_URL = "https://kmc.gos.pk"
    LISTING_URL = "https://kmc.gos.pk/"

    MAX_WORKERS = 5

    # Essential: Include 'Referer' to prevent KMC server from returning 403 Forbidden
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://kmc.gos.pk/",
    }

    @staticmethod
    def parse_date(date_str: str, article_url: str = "") -> datetime:
        """
        Parses dates like 'September 7, 2026' into UTC datetime.
        Falls back to regex extraction from the URL path (/YYYY/MM/DD/).
        """
        if date_str:
            date_str = date_str.strip()
            formats = [
                "%B %d, %Y",       # September 7, 2026
                "%b %d, %Y",       # Sep 7, 2026
                "%d %B %Y",       # 07 September 2026
                "%d %b %Y",       # 07 Sep 2026
                "%Y-%m-%d",       # 2026-09-07
                "%d/%m/%Y",       # 07/09/2026
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    if not dt.tzinfo:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except Exception:
                    continue

        # Fallback: Extract date from URL path (e.g., /2026/09/07/)
        if article_url:
            match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", article_url)
            if match:
                try:
                    dt = datetime.strptime(
                        f"{match.group(1)}-{match.group(2)}-{match.group(3)}", "%Y-%m-%d"
                    )
                    return dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

        logger.warning(f"Could not parse date '{date_str}', defaulting to now(UTC)")
        return datetime.now(timezone.utc)

    @staticmethod
    def fetch_article_details(article_url: str):
        """
        Visits the article page and extracts:
        - title:   .elementor-widget-theme-post-title h1
        - author:  "Karachi Metropolitan Corporation" (website name)
        - date:    .elementor-position-inline-end .elementor-icon-box-title span
        - content: .elementor-widget-theme-post-content
        """
        try:
            logger.info(f"Fetching KMC article: {article_url}")

            response = requests.get(
                article_url,
                timeout=25,
                verify=False,
                headers=get_random_headers(KMCPipeline.HEADERS),
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # 1. Title
            title_elem = (
                soup.select_one(".elementor-widget-theme-post-title h1")
                or soup.select_one("h1.elementor-heading-title")
            )
            title = title_elem.get_text(strip=True) if title_elem else ""

            if not title:
                logger.warning(f"No title found for {article_url}")
                return None

            # 2. Author (website name)
            author = KMCPipeline.WEBSITE_NAME

            # 3. Datetime
            date_elem = soup.select_one(".elementor-position-inline-end .elementor-icon-box-title span")
            date_str = date_elem.get_text(strip=True) if date_elem else ""
            pub_date = KMCPipeline.parse_date(date_str, article_url=article_url)

            # 4. Content
            content_div = (
                soup.select_one(".elementor-widget-theme-post-content")
                or soup.select_one(".news-desc")
            )

            if not content_div:
                logger.warning(f"No content found for {article_url}")
                return None

            # Remove unwanted tags
            for tag in content_div(["script", "style", "iframe", "figure", "noscript"]):
                tag.decompose()

            for a in content_div.find_all("a"):
                a.unwrap()

            paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p") if p.get_text(strip=True)]
            if paragraphs:
                clean_content = "\n\n".join(paragraphs)
            else:
                clean_content = content_div.get_text(separator="\n\n", strip=True)

            clean_content = re.sub(r"http\S+|www\.\S+", "", clean_content).strip()

            if len(clean_content) < 100:
                logger.info(f"Skipped article '{title}' (content length < 100 chars)")
                return None

            return {
                "title": title,
                "authors": author,
                "articlePubDate": pub_date,
                "content": clean_content,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch KMC article {article_url}: {e}")
            return None

    @staticmethod
    def get_article_links():
        """
        Fetches the KMC homepage and extracts news URLs
        using selector: .post-inner h2 a
        """
        try:
            logger.info(f"Fetching KMC listing: {KMCPipeline.LISTING_URL}")

            response = requests.get(
                KMCPipeline.LISTING_URL,
                timeout=30,
                verify=False,
                headers=get_random_headers(KMCPipeline.HEADERS),
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Extract headline links
            link_elements = soup.select(".post-inner h2 a")
            urls = []
            seen = set()

            for a in link_elements:
                href = a.get("href")
                if not href:
                    continue

                full_url = urljoin(KMCPipeline.BASE_URL, href)
                if full_url not in seen:
                    seen.add(full_url)
                    urls.append(full_url)

            logger.info(f"Discovered {len(urls)} article links on KMC portal.")
            return urls

        except Exception as e:
            logger.error(f"Failed to fetch KMC listing: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        article_links = KMCPipeline.get_article_links()
        if not article_links:
            return []

        articles = []
        feed_build_date = datetime.now(timezone.utc)

        # Concurrently fetch article contents
        with ThreadPoolExecutor(max_workers=KMCPipeline.MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(KMCPipeline.fetch_article_details, url): url
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
                        "language": "ur-PK",
                        "source": KMCPipeline.SOURCE,
                        "content": details["content"],
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": ["Karachi", "Government", "KMC", "Sindh"],
                    })

                except Exception as e:
                    logger.warning(f"Error resolving article {url}: {e}")
                    continue

        logger.info(f"Successfully processed {len(articles)} KMC articles.")
        return articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = KMCPipeline.process_input()

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
            logger.error(f"KMC pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }