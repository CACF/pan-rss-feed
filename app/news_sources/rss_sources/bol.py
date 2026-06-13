import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import MongoDBClient, get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class BOLNewsBusinessRSSPipeline:

    SOURCE = "BOLNews"
    RSS_FEEDS = [
        "https://www.bolnews.com/category/business/feed/",
        "https://www.bolnews.com/category/sports/feed/"
    ]

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        try:
            dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception:
            logger.warning(f"Unrecognized date format: {date_str}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """
        Clean WordPress RSS HTML
        """
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(
                ["script", "style", "iframe", "noscript", "img", "figure"]
            ):
                tag.decompose()

            text = soup.get_text(separator=" ")

            text = re.sub(
                r"The post .*? appeared first on .*?\.",
                "",
                text,
                flags=re.IGNORECASE,
            )
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def full_description(article_url):
        """
        Scrape full article content from BOL News article page
        CSS selector:
        .elementor-widget-theme-post-content p
        """
        if not article_url:
            return ""

        try:
            logger.info(f"Fetching full article: {article_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    article_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
                try:
                    response.raise_for_status()
                    html = response.text
                finally:
                    response.close()

            soup = BeautifulSoup(html, "html.parser")

            paragraphs = soup.select(
                ".elementor-widget-theme-post-content p"
            )

            if not paragraphs:
                logger.warning(
                    f"No article body found for URL: {article_url}"
                )
                return ""

            content_parts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    content_parts.append(text)

            content = " ".join(content_parts)

            content = re.sub(
                r"The post .*? appeared first on .*?\.",
                "",
                content,
                flags=re.IGNORECASE,
            )
            content = re.sub(r"http\S+|www\.\S+", "", content)

            return " ".join(content.split())

        except Exception as e:
            logger.error(
                f"Failed to scrape full article from {article_url}: {e}"
            )
            return ""

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching BOL News RSS feed: {feed_url}")

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
                    author_elem = item.find("dc:creator")
                    desc_elem = item.find("description")
                    category_elems = item.find_all("category")
                    image_elems = None

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    pub_date = (
                        BOLNewsBusinessRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    content = BOLNewsBusinessRSSPipeline.full_description(
                        link
                    )

                    if len(content) < 150 and desc_elem:
                        content = (
                            BOLNewsBusinessRSSPipeline.clean_content(
                                desc_elem.get_text()
                            )
                        )

                    if len(content) < 150:
                        logger.info(
                            f"Skipped article '{title}' (content < 150 chars)"
                        )
                        continue

                    authors = (
                        author_elem.get_text(strip=True)
                        if author_elem
                        else "BOL News Business Desk"
                    )

                    tags = [
                        cat.get_text(strip=True)
                        for cat in category_elems
                        if cat.get_text(strip=True).lower() != "business"
                    ]
                    

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": authors,
                        "language": "en-US",
                        "image" : image_elems,
                        "source": BOLNewsBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": (
                                    "Business"
                                    if "business" in feed_url.lower()
                                    else "Sports"
                                    if "sports" in feed_url.lower()
                                    else ""
                                ),
                        "media_origin": "local",
                        "tags": tags,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"Failed to process BOL News item: {e}"
                    )
                    continue

            logger.info(
                f"Parsed {len(articles)} BOL News business articles."
            )
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed_url in BOLNewsBusinessRSSPipeline.RSS_FEEDS:
                articles = BOLNewsBusinessRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"Tribune RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }