from config import SPORTS_TABLE
import re
import uuid
import logging
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import concurrent.futures
import cloudscraper

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class FIHHockeyScraper:
    SOURCE = "FIH Hockey"
    BASE_URL = "https://www.fih.hockey"
    LISTING_URL = "https://www.fih.hockey/news"
    MAX_LOOKBACK_DAYS = 7  # only keep/scrape articles within this window
    MAX_PAGES_HARD_CAP = 50  # safety cap so a bug can't loop forever

    HEADERS = {"Accept-Language": "en-US,en;q=0.9"}
    SCRAPER = cloudscraper.create_scraper()

    @staticmethod
    def parse_date(date_str: str):
        """Parse FIH Hockey date strings to datetime."""
        if not date_str:
            return datetime.now(timezone.utc)
        try:
            clean_str = date_str.strip()
            try:
                dt = datetime.fromisoformat(clean_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass
            # Fallback for formats like "14 July, 2026" / "14 Jul, 2026"
            for fmt in ("%d %B, %Y", "%d %b, %Y", "%d %B %Y", "%d %b %Y"):
                try:
                    dt = datetime.strptime(clean_str, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            raise ValueError(f"Unrecognized date format: {clean_str}")
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
    def fetch_listing_page_links(cls, page_num):
        """Fetch article links from a single /news listing page."""
        page_url = (
            cls.LISTING_URL if page_num == 1 else f"{cls.LISTING_URL}/page/{page_num}"
        )

        logger.info(f"Fetching article links from: {page_url}")

        resp = cls.SCRAPER.get(
            page_url,
            timeout=30,
            headers=get_random_headers(cls.HEADERS),
        )

        try:
            resp.raise_for_status()
            payload = resp.content
        finally:
            resp.close()

        soup = BeautifulSoup(payload, "html.parser")

        article_links = soup.select("div.article-content > a[href]")

        if not article_links:
            logger.warning(
                f"No article links found on page {page_num} "
                f"(status={resp.status_code})"
            )
            return []

        links = []

        for a in article_links:
            href = a.get("href")

            if not href:
                continue

            if href.startswith("/"):
                href = cls.BASE_URL + href

            links.append(href)

        return list(dict.fromkeys(links))

    def format_iso8601(dt: datetime) -> str:
        iso_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        offset = dt.strftime("%z")
        return iso_date + offset[:-2] + ":" + offset[-2:]

    @staticmethod
    def fetch_full_description(soup):
        body = soup.select_one("div.article-body")

        if not body:
            return ""

        texts = []

        for tag in body.select("p, li"):
            text = tag.get_text(" ", strip=True)
            if text:
                texts.append(text)

        return "\n\n".join(texts)

    @classmethod
    def fetch_article(cls, url):
        """Fetch and parse an individual FIH Hockey article."""
        try:
            resp = cls.SCRAPER.get(
                url, timeout=30, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")

            title_elem = soup.select_one("h2.article-title")
            title = title_elem.get_text(strip=True) if title_elem else ""

            date_elem = soup.select_one(
                ".head-wrap > .article-meta > .meta-date > span"
            )
            date_str = date_elem.get_text(strip=True) if date_elem else ""
            pub_date = (
                cls.parse_date(date_str) if date_str else datetime.now(timezone.utc)
            )

            author = "Unknown"

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
                "language": "en",
                "source": cls.SOURCE,
                "content": content,
                "genre": "Hockey",
                "media_origin": "local",
                "tags": [],
            }
            return article
        except Exception as e:
            logger.warning(f"Failed to fetch FIH Hockey article {url}: {e}")
            return None

    @classmethod
    def fetch_recent_articles(cls):
        """
        Walk listing pages one at a time, fetching each page's articles and
        checking their dates. Stops as soon as a page yields zero articles
        within MAX_LOOKBACK_DAYS — assumes the listing is newest-first, so
        once we hit stale articles there's no point scraping further pages.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=cls.MAX_LOOKBACK_DAYS)
        all_articles = []
        seen_urls = set()

        for page_num in range(1, cls.MAX_PAGES_HARD_CAP + 1):
            links = cls.fetch_listing_page_links(page_num)
            if not links:
                logger.info(f"No links on page {page_num}, stopping pagination")
                break

            new_links = [l for l in links if l not in seen_urls]
            if not new_links:
                logger.info(f"No new links on page {page_num}, stopping pagination")
                break
            seen_urls.update(new_links)

            page_articles = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(cls.fetch_article, url): url for url in new_links
                }
                for future in concurrent.futures.as_completed(futures):
                    article = future.result()
                    if article:
                        page_articles.append(article)

            recent_on_page = [a for a in page_articles if a["articlePubDate"] >= cutoff]
            all_articles.extend(recent_on_page)

            logger.info(
                f"Page {page_num}: {len(page_articles)} fetched, "
                f"{len(recent_on_page)} within {cls.MAX_LOOKBACK_DAYS} days"
            )

            if not recent_on_page:
                logger.info(
                    f"Page {page_num} had no articles within {cls.MAX_LOOKBACK_DAYS} "
                    f"days, stopping pagination"
                )
                break

        return all_articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or SPORTS_TABLE
            all_articles = FIHHockeyScraper.fetch_recent_articles()

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
            logger.error(f"FIH Hockey pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
