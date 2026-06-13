import re
import uuid
import os
import logging
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import cloudscraper
import concurrent.futures

from app.utilities import MongoDBClient, get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ZameenRSSPipeline:

    SOURCE = "Zameen"
    RSS_FEEDS = [
        "https://www.zameen.com/blog/feed/",
        "https://www.zameen.com/news/feed/",
        "https://www.zameen.com/blog/property/feed/",
        "https://www.zameen.com/blog/construction/feed/",
        "https://www.zameen.com/blog/real-estate-trends/feed/",
        "https://www.zameen.com/blog/lifestyle/feed/",
        "https://www.zameen.com/blog/home-decor/feed/",
        "https://www.zameen.com/blog/laws-taxes/feed/",
        "https://www.zameen.com/blog/tourism/feed/",
        "https://www.zameen.com/blog/economy/feed/",
    ]

    @staticmethod
    def parse_date(date_str):
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
    def clean_content(content_html):
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
    def full_description(link):
        """
        Fetch full Zameen article content and tags (author comes from RSS feed).
        """
        content = None
        tags = []

        if not link:
            return None, tags

        try:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    link,
                    timeout=30,
                    headers=get_random_headers(),
                )
                response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")
            paragraphs = []
            for p in soup.select("article.blog_post_container div.entry-content.blog_post_text.blog_post_description p"):
                text = p.get_text(strip=True)
                if len(text) < 30:
                    continue
                paragraphs.append(ZameenRSSPipeline.clean_content(text))

            if paragraphs:
                content = " ".join(paragraphs)

            for tag_elem in soup.select("a[rel='tag']"):
                tag_text = tag_elem.get_text(strip=True)
                if tag_text and tag_text not in tags:
                    tags.append(tag_text)

        except Exception as e:
            logger.warning(f"Failed to fetch Zameen article {link}: {e}")

        return content, tags

    @staticmethod
    def fetch_rss_feed(feed_url, seen_links):
        """
        Fetch and parse Zameen RSS feed (author taken from feed, content & tags from article page).
        """
        try:
            logger.info(f"Fetching Zameen RSS feed: {feed_url}")

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

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    guid_elem = item.find("guid")
                    pubdate_elem = item.find("pubDate")
                    creator_elem = item.find("dc:creator")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    unique_key = guid_elem.get_text(strip=True) if guid_elem else link
                    if unique_key in seen_links:
                        continue
                    seen_links.add(unique_key)

                    pub_date = ZameenRSSPipeline.parse_date(
                        pubdate_elem.get_text() if pubdate_elem else None
                    )
                    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
                    if pub_date < cutoff:
                        continue

                    author = creator_elem.get_text(strip=True) if creator_elem else "Zameen"

                    content, categories = ZameenRSSPipeline.full_description(link)
                    if not content or len(content) < 200:
                        continue

                    article = {
                        "id": unique_key,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "source": ZameenRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Real Estate",
                        "media_origin": "pakistan",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process Zameen article: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Zameen RSS feed: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """
        Fetch articles from all RSS feeds concurrently.
        """
        all_articles = []
        seen_links = set()
        logger.info("Starting Zameen RSS pipeline (concurrent)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(ZameenRSSPipeline.fetch_rss_feed, feed, seen_links)
                       for feed in ZameenRSSPipeline.RSS_FEEDS]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Failed to process Zameen feed")

        logger.info(f"Total Zameen articles processed: {len(all_articles)}")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = ZameenRSSPipeline.process_input()

            # Deduplicate by id
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
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

            result = SupabaseClient.insert_articles(all_articles)

            return result

        except Exception as e:
            logger.error(f"Zameen RSS pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }