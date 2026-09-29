from datetime import datetime, timezone
import hashlib
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

# Disable SSL insecure request warnings for government portals
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class CommissionerKarachiPipeline:

    SOURCE = "CommissionerKarachi"
    WEBSITE_NAME = "Commissioner Karachi"
    BASE_URL = "https://commissionerkarachi.gos.pk"
    LISTING_URL = "https://commissionerkarachi.gos.pk/karachi/media-gallery"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://commissionerkarachi.gos.pk/",
    }

    @staticmethod
    def parse_date(date_str: str) -> datetime:
        """Parses dates formatted as DD/MM/YYYY into UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        date_str = date_str.strip()

        formats = [
            "%d/%m/%Y",  # 25/09/2026
            "%d-%m-%Y",  # 25-09-2026
            "%Y-%m-%d",  # 2026-09-25
            "%d %B %Y",  # 25 September 2026
            "%d %b %Y",  # 25 Sep 2026
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: '{date_str}', defaulting to now(UTC)")
        return datetime.now(timezone.utc)

    @staticmethod
    def fetch_page_content(url: str) -> str:
        """Fetches HTML with SSL verification bypassed if needed."""
        response = requests.get(
            url,
            timeout=30,
            verify=False,
            headers=get_random_headers(CommissionerKarachiPipeline.HEADERS),
        )
        response.raise_for_status()
        return response.content

    @staticmethod
    def process_input(input_data=None):
        """
        Parses all news items directly from cards on media-gallery:
        - Container: .card
        - Title:     .card-title
        - Content:   .card-body p.card-text (first p tag)
        - Date:      .card-body p.card-text (second p tag)
        - Author:    "Commissioner Karachi"
        """
        try:
            logger.info(f"Fetching Commissioner Karachi gallery: {CommissionerKarachiPipeline.LISTING_URL}")
            html_content = CommissionerKarachiPipeline.fetch_page_content(CommissionerKarachiPipeline.LISTING_URL)
            soup = BeautifulSoup(html_content, "html.parser")

            cards = soup.select(".card")
            logger.info(f"Discovered {len(cards)} news cards on Commissioner Karachi portal.")

            articles = []
            feed_build_date = datetime.now(timezone.utc)

            for card in cards:
                # 1. Title
                title_elem = card.select_one(".card-title")
                title = title_elem.get_text(strip=True) if title_elem else ""

                if not title:
                    continue

                # 2. Content & Date paragraphs
                p_tags = card.select(".card-body p.card-text")
                content = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else ""
                date_str = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else ""

                # Clean content text
                content = re.sub(r"http\S+|www\.\S+", "", content)
                content = re.sub(r"#\S+", "", content).strip()

                if len(content) < 30:
                    continue

                # 3. Publish Date
                pub_date = CommissionerKarachiPipeline.parse_date(date_str)

                # 4. Deterministic unique ID based on title and date
                unique_hash = hashlib.md5(f"{title}_{date_str}".encode("utf-8")).hexdigest()
                item_id = f"{CommissionerKarachiPipeline.BASE_URL}/news/{unique_hash}"

                articles.append({
                    "id": item_id,
                    "article_id": str(uuid.uuid4()),
                    "articlePubDate": pub_date,
                    "feedBuildDate": feed_build_date,
                    "title": title,
                    "authors": CommissionerKarachiPipeline.WEBSITE_NAME,
                    "language": "en-US",
                    "source": CommissionerKarachiPipeline.SOURCE,
                    "content": content,
                    "genre": "Karachi",
                    "media_origin": "local",
                    "tags": ["Karachi", "Government", "Sindh", "Administration"],
                })

            logger.info(f"Successfully parsed {len(articles)} articles from Commissioner Karachi.")
            return articles

        except Exception as e:
            logger.error(f"Failed to process Commissioner Karachi media gallery: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = CommissionerKarachiPipeline.process_input()

            # Deduplicate by ID
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            # Insert into Supabase
            return SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

        except Exception as e:
            logger.error(f"Commissioner Karachi pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }