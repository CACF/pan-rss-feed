from config import FASHION_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class WhoWhatWearFashionRSSPipeline:
    """
    Who What Wear Fashion RSS Pipeline

    The RSS feed itself (https://www.whowhatwear.com/feeds.xml) exposes a
    native <description> tag, so that field is taken directly from RSS.

    However, per instructions, author / datetime / full content are instead
    scraped from each article page directly using these CSS selectors:
        - author   -> span.vds-author a.vds-author__link
        - datetime -> span.vds-date time[data-component-name="UI:DateTime"]
        - content  -> #article-body > p

    This mirrors the Marie Claire pipeline's structure (same Future plc
    site family), so the same selectors apply here as well.
    """

    SOURCE = "WhoWhatWear"

    RSS_FEEDS = [
        "https://www.whowhatwear.com/feeds.xml",
    ]

    # -----------------------------
    # DATE PARSER
    # -----------------------------
    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        date_str = date_str.strip()

        # Try ISO 8601 first (e.g. datetime="2026-09-02T07:00:00Z" or with offset)
        try:
            iso_str = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_str)
            return (
                dt.astimezone(timezone.utc)
                if dt.tzinfo
                else dt.replace(tzinfo=timezone.utc)
            )
        except Exception:
            pass

        # Fallback to RSS-style pubDate formats
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return (
                    dt.astimezone(timezone.utc)
                    if dt.tzinfo
                    else dt.replace(tzinfo=timezone.utc)
                )
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    # -----------------------------
    # CLEAN CONTENT
    # -----------------------------
    @staticmethod
    def clean_text(html):
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            # remove noise
            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    # -----------------------------
    # FASHION FILTER
    # -----------------------------
    @staticmethod
    def is_fashion_related(title, categories):
        keywords = [
            "fashion",
            "style",
            "bridal",
            "outfit",
            "designer",
            "collection",
            "trend",
            "runway",
            "beauty",
            "makeup",
            "wedding",
            "couture",
            "clothing",
            "sweater",
            "shoe",
            "sneaker",
            "shopping",
            "gift",
        ]

        title_lower = title.lower()

        if any(k in title_lower for k in keywords):
            return True

        # Who What Wear uses categories like Fashion, Shopping, Beauty, etc.
        allowed_categories = {"fashion", "shopping", "beauty", "style"}

        for cat in categories:
            if cat.lower() in allowed_categories:
                return True

        return False

    # -----------------------------
    # FETCH RSS (LINKS + TITLES + DESCRIPTION + CATEGORIES)
    # -----------------------------
    @staticmethod
    def fetch_rss_links(feed_url):
        try:
            logger.info(f"Fetching Who What Wear RSS: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
                response.raise_for_status()
                payload = response.content

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            links = []

            for item in items:
                try:
                    title = (
                        item.find("title").get_text(strip=True)
                        if item.find("title")
                        else None
                    )
                    link = (
                        item.find("link").get_text(strip=True)
                        if item.find("link")
                        else None
                    )

                    if not title or not link:
                        continue

                    categories = [
                        c.get_text(strip=True) for c in item.find_all("category") if c
                    ]

                    # Description is native to the RSS feed - use it directly
                    description = (
                        item.find("description").get_text(strip=True)
                        if item.find("description")
                        else ""
                    )

                    links.append(
                        {
                            "title": title,
                            "link": link,
                            "categories": categories,
                            "description": description,
                        }
                    )

                except Exception as e:
                    logger.warning(f"Failed parsing RSS item: {e}")
                    continue

            logger.info(f"Found {len(links)} Who What Wear links.")
            return links

        except Exception as e:
            logger.error(f"Who What Wear RSS fetch failed: {e}")
            return []

    # -----------------------------
    # SCRAPE ARTICLE PAGE
    # -----------------------------
    @staticmethod
    def scrape_article(link):
        try:
            logger.info(f"Scraping Who What Wear article: {link}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    link,
                    timeout=30,
                    headers=get_random_headers(),
                )
                response.raise_for_status()
                html = response.text

            soup = BeautifulSoup(html, "html.parser")

            # --- Author ---
            author_tag = soup.select_one("span.vds-author a.vds-author__link")
            author = author_tag.get_text(strip=True) if author_tag else "Who What Wear"

            # --- Datetime ---
            date_tag = soup.select_one(
                'span.vds-date time[data-component-name="UI:DateTime"]'
            )
            date_str = None
            if date_tag:
                date_str = date_tag.get("datetime") or date_tag.get_text(strip=True)
            pub_date = WhoWhatWearFashionRSSPipeline.parse_date(date_str)

            # --- Content ---
            paragraphs = soup.select("#article-body > p")
            content_raw = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
            content = WhoWhatWearFashionRSSPipeline.clean_text(content_raw)

            return {
                "author": author,
                "pub_date": pub_date,
                "content": content,
            }

        except Exception as e:
            logger.warning(f"Failed scraping article {link}: {e}")
            return None

    # -----------------------------
    # RUN PIPELINE
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = FASHION_TABLE
            all_articles = []

            for feed_url in WhoWhatWearFashionRSSPipeline.RSS_FEEDS:
                rss_links = WhoWhatWearFashionRSSPipeline.fetch_rss_links(feed_url)

                for entry in rss_links:
                    title = entry["title"]
                    link = entry["link"]
                    categories = entry["categories"]
                    description = entry["description"]

                    # fashion filter (based on title/categories from RSS)
                    if not WhoWhatWearFashionRSSPipeline.is_fashion_related(
                        title, categories
                    ):
                        continue

                    scraped = WhoWhatWearFashionRSSPipeline.scrape_article(link)
                    if not scraped:
                        continue

                    content = scraped["content"]

                    # skip short articles
                    if len(content) < 200:
                        continue

                    feed_build_date = datetime.now(timezone.utc)

                    all_articles.append(
                        {
                            "id": link,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": scraped["pub_date"],
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "description": description,
                            "authors": scraped["author"],
                            "language": "en-US",
                            "image": None,
                            "source": WhoWhatWearFashionRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Fashion",
                            "media_origin": "local",
                            "tags": categories,
                        }
                    )

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles_current_year(
                all_articles, table_name=target_table
            )

        except Exception as e:
            logger.error(f"Who What Wear pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }