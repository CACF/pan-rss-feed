import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class PakistanRealEstateTimesRSSPipeline:

    SOURCE = "Pakistan Real Estate Times"
    RSS_FEEDS = [
        "https://www.pakrealestatetimes.com/syndication.php",
    ]

    @staticmethod
    def parse_date(date_str):
        """
        Parse RSS pubDate format:
        Example:
        Thu, 14 Nov 2024 06:51:41 +0000
        """
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception:
            logger.warning(f"Unrecognized RSS date format: {date_str}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean HTML:
        - Remove scripts, styles, iframes, figures, images
        - Remove URLs
        - Normalize whitespace
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse Pakistan Real Estate Times RSS feed."""
        try:
            logger.info(f"Fetching Pakistan Real Estate Times RSS feed: {feed_url}")

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
                    content_elem = item.find("content:encoded")
                    description_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = PakistanRealEstateTimesRSSPipeline.parse_date(
                        pub_date_elem.get_text() if pub_date_elem else None
                    )

                    author_name = "Pakistan Real Estate Times"
                    raw_content = ""
                    if content_elem:
                        raw_content = content_elem.get_text()
                    elif description_elem:
                        raw_content = description_elem.get_text()

                    content = PakistanRealEstateTimesRSSPipeline.clean_content(
                        raw_content
                    )

                    if len(content) < 150:
                        logger.info(
                            f"Skipped article '{title}' (content < 150 chars)"
                        )
                        continue

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author_name,
                        "language": "en-US",
                        "source": PakistanRealEstateTimesRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Real Estate",
                        "media_origin": "pakistan",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process Pakistan Real Estate Times item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} Pakistan Real Estate Times articles."
            )
            return articles

        except Exception as e:
            logger.error(
                f"Failed to fetch Pakistan Real Estate Times RSS feed: {e}"
            )
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run Pakistan Real Estate Times RSS pipeline and insert into MongoDB."""
        try:
            all_articles = []

            for feed_url in PakistanRealEstateTimesRSSPipeline.RSS_FEEDS:
                articles = PakistanRealEstateTimesRSSPipeline.fetch_rss_feed(
                    feed_url
                )
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles,
                user_email=input_data.get("email") if input_data else None,
            )

            return result

        except Exception as e:
            logger.error(
                f"Pakistan Real Estate Times RSS pipeline failed: {e}"
            )
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }