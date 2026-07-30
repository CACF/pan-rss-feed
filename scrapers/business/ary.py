from config import BUSINESS_TABLE
import re
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ARYNewsBusinessRSSPipeline:

    SOURCE = "ARYNews"
    RSS_FEEDS = [
        "https://arynews.tv/category/business/feed/",
    ]

    # How many article pages to fetch in parallel when pulling full bodies
    MAX_WORKERS = 8

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
    def fetch_full_description(link):
        """
        Fetch the full article body from the ARY News article page.

        ARY's RSS feed only ever includes a short teaser in <description>
        (a lead image + the title repeated) and never populates
        <content:encoded>, so the real body has to be scraped from the
        article page itself. The site runs on the tagDiv "Newspaper"
        WordPress theme (identifiable by the "AA / Resize" font-size
        widget on article pages), whose article body normally lives in
        `div.td-post-content`. A few fallback selectors are tried in case
        that changes.
        """
        try:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    link,
                    timeout=30,
                    headers=get_random_headers(),
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

            # Strip share widgets / tags / related-posts blocks that can
            # live inside the same container as the body text.
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
        """Fetch and parse ARY News Business RSS feed."""
        try:
            logger.info(f"Fetching ARY News RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            # First pass: collect lightweight metadata for every item
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

                    pub_date = (
                        ARYNewsBusinessRSSPipeline.parse_date(pub_date_elem.get_text())
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
                                else "ARY News Business Desk"
                            ),
                        }
                    )
                except Exception as e:
                    logger.warning(f"Failed to process ARY News article item: {e}")
                    continue

            # Second pass: fetch full article bodies in parallel
            # (RSS never carries the full body for this source).
            articles = []
            with ThreadPoolExecutor(
                max_workers=ARYNewsBusinessRSSPipeline.MAX_WORKERS
            ) as executor:
                future_to_stub = {
                    executor.submit(
                        ARYNewsBusinessRSSPipeline.fetch_full_description, stub["link"]
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
                        "source": ARYNewsBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

            logger.info(f"Parsed {len(articles)} ARY News business articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch ARY News RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = BUSINESS_TABLE
            all_articles = []

            for feed_url in ARYNewsBusinessRSSPipeline.RSS_FEEDS:
                articles = ARYNewsBusinessRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table
            )

            return result

        except Exception as e:
            logger.error(f"ARY News RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
