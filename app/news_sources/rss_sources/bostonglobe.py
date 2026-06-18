import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class BostonGlobeSportsRSSPipeline:
    """
    Boston Globe Sports RSS Pipeline
    """

    SOURCE = "BostonGlobe"
    RSS_FEEDS = [
        "https://www.bostonglobe.com/arc/outboundfeeds/rss/section/sports/?outputType=xml",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate into UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
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
    def clean_text(html):
        """Convert HTML → clean text."""
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    @staticmethod
    def clean_author(author_text: str) -> str:
        """
        Boston Globe uses dc:creator (often plain name).
        """
        if not author_text:
            return "Boston Globe Sports Desk"

        return author_text.strip()

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse RSS feed."""
        try:
            logger.info(f"Fetching RSS: {feed_url}")

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
            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")

                    creator_elem = item.find("dc:creator")
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        BostonGlobeSportsRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else desc_elem.get_text() if desc_elem else ""
                    )

                    content = BostonGlobeSportsRSSPipeline.clean_text(raw_content)
                    media_elem = item.find("media:content")
                    image_url = media_elem.get("url") if media_elem else None

                    if len(content) < 200:
                        logger.info(f"Skipped (too short): {title}")
                        continue

                    author = BostonGlobeSportsRSSPipeline.clean_author(
                        creator_elem.get_text(strip=True) if creator_elem else ""
                    )

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "image": image_url,
                        "language": "en-US",
                        "source": BostonGlobeSportsRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Sports",
                        "media_origin": "International",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles.")
            return articles

        except Exception as e:
            logger.error(f"RSS fetch failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in BostonGlobeSportsRSSPipeline.RSS_FEEDS:
                articles = BostonGlobeSportsRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
