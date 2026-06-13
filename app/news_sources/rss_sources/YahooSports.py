import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class YahooSportsRSSPipeline:

    SOURCE = "Yahoo Sports"
    RSS_FEEDS = [
        "https://sports.yahoo.com/rss/"
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS date formats safely."""
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_html(html):
        """
        Clean Yahoo RSS content:
        - remove scripts/styles
        - unwrap links
        - remove tracking attributes
        - remove raw URLs
        """
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript"]):
                tag.decompose()

            for a in soup.find_all("a"):
                a.unwrap()

            text = soup.get_text(separator=" ")

            # remove urls
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"HTML cleaning failed: {e}")
            return html or ""

    @staticmethod
    def extract_image(item):
        """
        Yahoo uses:
        - media:content
        - embedded <img> in content:encoded
        """
        try:
            # 1. media:content (preferred)
            media = item.find("media:content")
            if media and media.get("url"):
                return media.get("url")

            # 2. fallback: image inside content:encoded
            content = item.find("content:encoded")
            if content:
                soup = BeautifulSoup(content.get_text(), "html.parser")
                img = soup.find("img")
                if img and img.get("src"):
                    return img.get("src")

            return None

        except Exception:
            return None

    @staticmethod
    def extract_content(item):
        """
        Prefer content:encoded → fallback description
        """
        content_elem = item.find("content:encoded")
        desc_elem = item.find("description")

        if content_elem:
            return YahooSportsRSSPipeline.clean_html(content_elem.get_text())

        if desc_elem:
            return YahooSportsRSSPipeline.clean_html(desc_elem.get_text())

        return ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch Yahoo Sports RSS feed."""
        try:
            logger.info(f"Fetching Yahoo RSS: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers()
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title = item.find("title").get_text(strip=True) if item.find("title") else None
                    link = item.find("link").get_text(strip=True) if item.find("link") else None

                    if not title or not link:
                        continue

                    pub_date = YahooSportsRSSPipeline.parse_date(
                        item.find("pubDate").get_text() if item.find("pubDate") else None
                    )

                    content = YahooSportsRSSPipeline.extract_content(item)

                    # skip low-quality items
                    if len(content) < 120:
                        continue

                    image = YahooSportsRSSPipeline.extract_image(item)

                    # author (Yahoo usually empty or inconsistent)
                    creator = item.find("dc:creator")
                    source = item.find("source")

                    author = (
                        creator.get_text(strip=True)
                        if creator and creator.get_text(strip=True)
                        else source.get_text(strip=True)
                        if source
                        else "Yahoo Sports"
                    )

                    # categories
                    categories = [
                        c.get_text(strip=True)
                        for c in item.find_all("category")
                        if c.get_text(strip=True)
                    ]

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "image": image,
                        "source": YahooSportsRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Sports",
                        "media_origin": "local",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed parsing Yahoo item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Yahoo articles")
            return articles

        except Exception as e:
            logger.error(f"Yahoo RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in YahooSportsRSSPipeline.RSS_FEEDS:
                articles = YahooSportsRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # deduplicate by link
            all_articles = list(
                {a["id"]: a for a in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            return SupabaseClient.insert_articles(all_articles)

        except Exception as e:
            logger.error(f"Yahoo pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }