import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class TribuneRSSPipeline:
    """
    Tribune RSS feed pipeline — Business category (Flask-compatible, single-threaded)
    Uses cloudscraper for Cloudflare-protected requests.
    """

    SOURCE = "Tribune"
    RSS_FEEDS = [
        "https://tribune.com.pk/feed/business",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime (UTC normalized)."""
        if not date_str:
            return datetime.now(timezone.utc)

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
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML into readable text:
        - Remove <a>, <script>, <style>, <iframe>, etc.
        - Remove visible URLs (http, https, www)
        - Collapse extra spaces
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            # Remove unwanted tags entirely
            for tag in soup(["script", "style", "iframe", "noscript"]):
                tag.decompose()

            # Remove anchor tags but keep their inner text
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()

            text = soup.get_text(separator=" ")

            # Remove visible URLs (plain links in text)
            text = re.sub(r"http\S+|www\.\S+", "", text)

            # Normalize spaces
            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse Tribune RSS feed using cloudscraper."""
        try:
            logger.info(f"Fetching Tribune RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(feed_url, timeout=30, headers=get_random_headers())
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
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    pub_date = (
                        TribuneRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    # Prefer full content if available
                    if content_elem:
                        content = TribuneRSSPipeline.clean_content(content_elem.get_text())
                    elif desc_elem:
                        content = TribuneRSSPipeline.clean_content(desc_elem.get_text())
                    else:
                        content = ""
                        
                    if len(content) < 200:
                        logger.info(f"Skipped article '{title}' due to content length < 200 chars")
                        continue
                    
                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": "Tribune Business Desk",
                        "language": "en-US",
                        "source": TribuneRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process Tribune article item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Tribune articles from feed.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Tribune RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run Tribune RSS pipeline and insert articles into MongoDB."""
        try:
            all_articles = []
            for feed_url in TribuneRSSPipeline.RSS_FEEDS:
                articles = TribuneRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles, user_email=input_data.get("email") if input_data else None
            )
            return result

        except Exception as e:
            logger.error(f"Tribune RSS pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
