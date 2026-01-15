import uuid
import logging
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.utilities import MongoDBClient, get_random_headers
import requests

logger = logging.getLogger(__name__)


class PBCNewsRSSPipeline:
    """
    Pakistan Business Council RSS feed pipeline
    """

    SOURCE = "Pakistan Business Council"
    RSS_FEEDS = [
        "https://www.pbc.org.pk/news/feed/",  # PBC RSS
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse PBC RSS date format to datetime object."""
        try:
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Convert HTML to clean plain text and remove links."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")

            # Remove unwanted elements
            for tag in soup(["script", "style", "iframe", "noscript", "form", "aside"]):
                tag.decompose()

            # Remove anchor tags but keep visible text
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()

            # Extract text
            text = soup.get_text(separator=" ", strip=True)

            # Remove any remaining URLs (e.g. http://, https://, www.)
            text = re.sub(r"http\S+|www\.\S+", "", text)

            # Normalize whitespace
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse PBC RSS feed."""
        try:
            logger.info(f"Fetching PBC RSS feed: {feed_url}")
            response = requests.get(feed_url, timeout=30, headers=get_random_headers())
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
                    guid_elem = item.find("guid")
                    author_elem = item.find("dc:creator")
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    pub_date = (
                        PBCNewsRSSPipeline.parse_date(item.find("pubDate").get_text())
                        if item.find("pubDate")
                        else datetime.now(timezone.utc)
                    )

                    # Clean content
                    content = ""
                    if content_elem:
                        content = PBCNewsRSSPipeline.clean_content(content_elem.get_text())
                    elif desc_elem:
                        content = PBCNewsRSSPipeline.clean_content(desc_elem.get_text())

                    # Skip articles with very short content
                    if len(content) < 200:
                        logger.info(f"Skipping short article ({len(content)} chars): {link}")
                        continue

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author_elem.get_text(strip=True)
                        if author_elem
                        else "Pakistan Business Council",
                        "language": "en-US",
                        "source": PBCNewsRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)
                except Exception as e:
                    logger.warning(f"Failed to process PBC article item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} valid articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch PBC RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run PBC RSS pipeline."""
        try:
            all_articles = []
            for feed_url in PBCNewsRSSPipeline.RSS_FEEDS:
                articles = PBCNewsRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles, user_email=input_data.get("email") if input_data else None
            )
            return result

        except Exception as e:
            logger.error(f"PBC RSS pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
