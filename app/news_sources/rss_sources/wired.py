import uuid
import logging
import concurrent.futures
import re
import requests

from datetime import datetime, timezone
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class WiredBusinessRSSPipeline:

    SOURCE = "WIRED"

    RSS_FEEDS = [
        "https://www.wired.com/feed/category/business/latest/rss/",
    ]

    BASE_HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate into UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S %z",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)

                return dt

            except ValueError:
                continue

        logger.warning(f"Unable to parse date: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(text):
        if not text:
            return ""

        text = re.sub(r"http\S+|www\.\S+", "", text)
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    @staticmethod
    def full_description(link):
        """
        Fetch full WIRED article content, author, tags and image.
        """

        content = None
        author = "WIRED Staff"
        tags = []
        image_url = None

        if not link:
            return None, author, tags, image_url

        try:
            response = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(
                    WiredBusinessRSSPipeline.BASE_HEADERS
                ),
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            paragraphs = []

            selectors = [
                "div.body__inner-container > p",
                ".body__inner-container p",
                "article p",
            ]

            for selector in selectors:
                for p in soup.select(selector):
                    text = p.get_text(" ", strip=True)

                    if len(text) < 30:
                        continue

                    paragraphs.append(
                        WiredBusinessRSSPipeline.clean_content(text)
                    )

                if paragraphs:
                    break

            if paragraphs:
                content = " ".join(paragraphs)

            author_elem = soup.select_one(
                'span[itemprop="name"] a.byline__name-link'
            )

            if not author_elem:
                author_elem = soup.select_one(".byline__name-link")

            if author_elem:
                author = author_elem.get_text(strip=True)

            for tag in soup.select("a[href*='/tag/']"):
                tag_text = tag.get_text(strip=True)

                if tag_text and tag_text not in tags:
                    tags.append(tag_text)

            img_elem = soup.select_one(
                "meta[property='og:image']"
            )

            if img_elem and img_elem.get("content"):
                image_url = img_elem["content"]

        except Exception as e:
            logger.warning(
                f"Failed to fetch WIRED article {link}: {e}"
            )

        return content, author, tags, image_url

    @staticmethod
    def fetch_wired_rss_feed(feed_url):
        """
        Fetch and parse a single WIRED RSS feed.
        """

        try:
            logger.info(f"Fetching WIRED RSS feed: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(
                    WiredBusinessRSSPipeline.BASE_HEADERS
                ),
            )
            response.raise_for_status()

            soup = BeautifulSoup(
                response.content,
                "lxml-xml"
            )

            items = soup.find_all("item")

            feed_build_date = datetime.now(timezone.utc)

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date_elem = item.find("pubDate")

                    article_pub_date = (
                        WiredBusinessRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else feed_build_date
                    )

                    (
                        content,
                        author,
                        tags,
                        image_url,
                    ) = WiredBusinessRSSPipeline.full_description(
                        link
                    )

                    if not content:
                        logger.info(
                            f"Skipping article with no content: {title}"
                        )
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en",
                        "source": WiredBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "foreign",
                        "tags": tags,
                        "image_url": image_url,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process WIRED item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} WIRED articles from {feed_url}"
            )

            return articles

        except Exception as e:
            logger.error(
                f"Failed to fetch WIRED RSS feed {feed_url}: {e}"
            )
            return []

    @staticmethod
    def process_input(input_data=None):
        """
        Process all WIRED feeds concurrently.
        """

        logger.info(
            "Starting WIRED RSS pipeline (concurrent)"
        )

        all_articles = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=5
        ) as executor:

            futures = [
                executor.submit(
                    WiredBusinessRSSPipeline.fetch_wired_rss_feed,
                    feed_url,
                )
                for feed_url in WiredBusinessRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(
                futures
            ):
                try:
                    all_articles.extend(
                        future.result()
                    )
                except Exception:
                    logger.exception(
                        "Failed to process WIRED feed"
                    )

        logger.info(
            f"Total WIRED articles processed: {len(all_articles)}"
        )

        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = (
                WiredBusinessRSSPipeline.process_input()
            )

            # Deduplicate by URL
            all_articles = list(
                {
                    article["id"]: article
                    for article in all_articles
                }.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            if not all_articles:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                    "message": "No articles found",
                }

            result = SupabaseClient.insert_articles(
                all_articles
            )

            return result

        except Exception as e:
            logger.error(
                f"WIRED RSS pipeline failed: {e}"
            )

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }