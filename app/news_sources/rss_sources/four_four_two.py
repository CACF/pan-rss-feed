import re
import uuid
import logging
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class FourFourTwoRSSPipeline:

    SOURCE = "FourFourTwo"

    RSS_FEEDS = [
        "https://www.fourfourtwo.com/feeds.xml",
    ]

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
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

        logger.warning(f"Unknown date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Convert HTML to clean text."""
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript"]):
                tag.decompose()

            for tag in soup.find_all("a"):
                tag.unwrap()

            text = soup.get_text(separator=" ")

            text = re.sub(r"http\S+|www\.\S+", "", text)
            text = re.sub(r"\s+", " ", text)

            return text.strip()

        except Exception as e:
            logger.warning(f"Failed cleaning content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch and parse FourFourTwo RSS feed."""
        try:
            logger.info(f"Fetching FourFourTwo RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    headers=get_random_headers(),
                    timeout=30,
                )

                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "xml")

            feed_build_date = (
                FourFourTwoRSSPipeline.parse_date(
                    soup.find("lastBuildDate").get_text()
                )
                if soup.find("lastBuildDate")
                else datetime.now(timezone.utc)
            )

            items = soup.find_all("item")

            articles = []

            for item in items:
                try:
                    title = item.title.get_text(strip=True) if item.title else ""
                    link = item.link.get_text(strip=True) if item.link else ""

                    if not title or not link:
                        continue

                    pub_date = (
                        FourFourTwoRSSPipeline.parse_date(
                            item.pubDate.get_text()
                        )
                        if item.pubDate
                        else datetime.now(timezone.utc)
                    )

                    author = "FourFourTwo"

                    creator = item.find("dc:creator")
                    if creator:
                        author = creator.get_text(strip=True)

                    content = ""

                    encoded = item.find("content:encoded")
                    if encoded:
                        content = FourFourTwoRSSPipeline.clean_content(
                            encoded.get_text()
                        )
                    elif item.description:
                        content = FourFourTwoRSSPipeline.clean_content(
                            item.description.get_text()
                        )

                    if len(content) < 200:
                        logger.info(
                            f"Skipped '{title}' because content < 200 characters"
                        )
                        continue

                    image_url = None

                    media = item.find("media:content")
                    if media and media.get("url"):
                        image_url = media["url"]

                    if not image_url:
                        thumb = item.find("media:thumbnail")
                        if thumb and thumb.get("url"):
                            image_url = thumb["url"]

                    if not image_url:
                        enclosure = item.find("enclosure")
                        if enclosure and enclosure.get("url"):
                            image_url = enclosure["url"]

                    tags = [
                        tag.get_text(strip=True)
                        for tag in item.find_all("category")
                    ]

                    genre = ""

                    tag_text = " ".join(tags).lower()

                    if "quiz" in tag_text:
                        genre = "Quiz"
                    elif "player" in tag_text:
                        genre = "Player"
                    elif "competition" in tag_text:
                        genre = "Competition"
                    else:
                        genre = "Sports"

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "image": image_url,
                        "source": FourFourTwoRSSPipeline.SOURCE,
                        "content": content,
                        "genre": genre,
                        "media_origin": "international",
                        "tags": tags,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process FourFourTwo article: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} FourFourTwo articles from feed."
            )

            return articles

        except Exception as e:
            logger.error(f"Failed to fetch FourFourTwo RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed in FourFourTwoRSSPipeline.RSS_FEEDS:
                all_articles.extend(
                    FourFourTwoRSSPipeline.fetch_rss_feed(feed)
                )

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"FourFourTwo pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }