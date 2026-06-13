import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class GuardianFootballRSSPipeline:

    SOURCE = "The Guardian"
    RSS_FEEDS = [
        "https://www.theguardian.com/football/rss"
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime (UTC normalized)."""
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
    def clean_content(html):
        """
        Clean RSS description:
        - remove HTML tags
        - remove links
        - normalize whitespace
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
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    @staticmethod
    def extract_image(item):
        """
        Extract image from media:content (Guardian uses multiple sizes)
        Prefer largest (700 width if available)
        """
        try:
            media_items = item.find_all("media:content")
            if not media_items:
                return None

            best = None
            best_width = 0

            for m in media_items:
                width = m.get("width")
                url = m.get("url")

                if not url:
                    continue

                try:
                    w = int(width) if width else 0
                except Exception:
                    w = 0

                if w > best_width:
                    best_width = w
                    best = url

            return best

        except Exception:
            return None

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch Guardian RSS feed."""
        try:
            logger.info(f"Fetching Guardian RSS: {feed_url}")

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
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    desc_elem = item.find("description")
                    pub_date_elem = item.find("pubDate")
                    creator_elem = item.find("dc:creator")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        GuardianFootballRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    content = ""
                    if desc_elem:
                        content = GuardianFootballRSSPipeline.clean_content(
                            desc_elem.get_text()
                        )

                    # skip very short content
                    if len(content) < 150:
                        continue

                    image = GuardianFootballRSSPipeline.extract_image(item)

                    author = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else "The Guardian"
                    )

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-GB",
                        "image": image,
                        "source": GuardianFootballRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Sports",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed parsing Guardian item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Guardian articles")
            return articles

        except Exception as e:
            logger.error(f"Guardian RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in GuardianFootballRSSPipeline.RSS_FEEDS:
                articles = GuardianFootballRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # dedupe by link
            all_articles = list(
                {a["id"]: a for a in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            return SupabaseClient.insert_articles(all_articles)

        except Exception as e:
            logger.error(f"Guardian pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }