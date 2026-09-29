from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import logging
import re
import uuid
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient
from config import KARACHI_TABLE

logger = logging.getLogger(__name__)


class ARYNewsKarachiRSSPipeline:

    SOURCE = "ARYNews"
    RSS_FEEDS = [
        "https://arynews.tv/category/karachi/feed/",  # Dedicated ARY Karachi News
    ]

    # How many article pages to fetch in parallel when pulling full bodies
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

        try:
            dt = datetime.fromisoformat(date_str.strip())
            if dt.tzinfo:
                return dt.astimezone(timezone.utc)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo:
                    return dt.astimezone(timezone.utc)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML into readable text:
        - Remove scripts, styles, iframes
        - Remove anchor tags but keep text
        - Remove visible URLs
        - Normalize whitespace
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript"]):
                tag.decompose()

            for a_tag in soup.find_all("a"):
                a_tag.unwrap()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def full_description(link):
        """
        Fetch the full article body from the ARY News article page.
        """
        if not link:
            return ""

        try:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    link,
                    timeout=30,
                    headers=get_random_headers(ARYNewsKarachiRSSPipeline.HEADERS),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "html.parser")

            container = (
                soup.select_one("div.post__content")
                or soup.select_one("div.td-post-content")
                or soup.select_one("div.tdb_single_content .tdb-block-inner")
                or soup.select_one("div[itemprop='articleBody']")
                or soup.select_one("div.entry-content")
            )

            if not container:
                logger.warning(f"No article body container found for {link}")
                return ""

            # Strip share widgets / tags / related-posts blocks
            for junk in container.select(
                "script, style, iframe, "
                ".td-post-source-tags, .td-post-sharing-bottom, "
                ".td_block_related_posts, .td-post-featured-image"
            ):
                junk.decompose()

            for a_tag in container.find_all("a"):
                a_tag.unwrap()

            paragraphs = container.find_all(["p", "li"])
            text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to fetch full description from {link}: {e}")
            return ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse ARY News Karachi RSS feed."""
        try:
            logger.info(f"Fetching ARY News RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(ARYNewsKarachiRSSPipeline.HEADERS),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            # 1. Collect lightweight metadata for each item
            stubs = []
            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    creator_elem = item.find("dc:creator")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                    ]
                    if not categories:
                        categories = ["Karachi", "Pakistan"]

                    pub_date = (
                        ARYNewsKarachiRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    stubs.append(
                        {
                            "title": title,
                            "link": link,
                            "pub_date": pub_date,
                            "authors": (
                                creator_elem.get_text(strip=True)
                                if creator_elem
                                else "ARY News Karachi Desk"
                            ),
                            "tags": categories[:10],
                        }
                    )
                except Exception as e:
                    logger.warning(f"Failed to process ARY News article item: {e}")
                    continue

            # 2. Fetch full article bodies in parallel
            articles = []
            with ThreadPoolExecutor(
                max_workers=ARYNewsKarachiRSSPipeline.MAX_WORKERS
            ) as executor:
                future_to_stub = {
                    executor.submit(
                        ARYNewsKarachiRSSPipeline.full_description, stub["link"]
                    ): stub
                    for stub in stubs
                }

                for future in as_completed(future_to_stub):
                    stub = future_to_stub[future]
                    title = stub["title"]
                    link = stub["link"]

                    try:
                        content = future.result()
                    except Exception as e:
                        logger.warning(f"Failed to fetch full body for '{title}': {e}")
                        content = ""

                    if len(content) < 200:
                        logger.info(f"Skipped article '{title}' (content < 200 chars)")
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": stub["pub_date"],
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": stub["authors"],
                        "language": "en-US",
                        "source": ARYNewsKarachiRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": stub["tags"],
                    }

                    articles.append(article)

            logger.info(f"Parsed {len(articles)} ARY News Karachi articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch ARY News RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = []

            for feed_url in ARYNewsKarachiRSSPipeline.RSS_FEEDS:
                articles = ARYNewsKarachiRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            # Database insertion into Supabase with category="karachi"
            return SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

        except Exception as e:
            logger.error(f"ARY News Karachi RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }