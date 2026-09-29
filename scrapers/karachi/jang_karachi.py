import concurrent.futures
from datetime import datetime, timezone
import logging
import re
import uuid
from bs4 import BeautifulSoup
import requests

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient
from config import KARACHI_TABLE

logger = logging.getLogger(__name__)


class JangNewsKarachiRSSPipeline:

    SOURCE = "Jang"
    RSS_FEEDS = [
        "https://jang.com.pk/rss/2/13",  # Karachi News (شہر قائد / کراچی)
    ]

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ur,en-US;q=0.9,en;q=0.8",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def clean_author(author_str, default="روزنامہ جنگ کراچی"):
        """Extract clean author name and strip dummy emails or parentheses."""
        if not author_str:
            return default

        match = re.search(r"\((.*?)\)", author_str)
        if match and match.group(1).strip():
            return match.group(1).strip()

        cleaned = re.sub(r"[\w\.-]+@[\w\.-]+", "", author_str).strip()
        cleaned = cleaned.strip("()[] -:")

        return cleaned if cleaned else default

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    @staticmethod
    def full_description(blog_url):
        """
        Requests the article link and extracts the full content 
        using the CSS selector: .detail_view_content > p
        """
        if not blog_url:
            return ""

        try:
            logger.info(f"Fetching full article page: {blog_url}")
            response = requests.get(
                blog_url,
                timeout=20,
                headers=get_random_headers(JangNewsKarachiRSSPipeline.HEADERS),
            )
            try:
                response.raise_for_status()
                html = response.content
            finally:
                response.close()

            soup = BeautifulSoup(html, "html.parser")

            # Try direct child selector first; fallback to descendant in case of nested markup
            paragraphs = soup.select(".detail_view_content > p")
            if not paragraphs:
                paragraphs = soup.select(".detail_view_content p")

            clean_paragraphs = [
                p.get_text(separator=" ", strip=True)
                for p in paragraphs
                if p.get_text(strip=True)
            ]

            full_text = "\n\n".join(clean_paragraphs)
            full_text = re.sub(r"http\S+|www\.\S+", "", full_text)

            return full_text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch full description from {blog_url}: {e}")
            return ""

    @staticmethod
    def clean_content(content_html):
        """Fallback content cleaner for RSS description snippets."""
        if not content_html:
            return ""

        try:
            soup = BeautifulSoup(content_html, "html.parser")

            for tag in soup(["script", "style", "iframe", "noscript", "img"]):
                tag.decompose()

            for a in soup.find_all("a"):
                a.unwrap()

            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @staticmethod
    def fetch_jang_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Jang News RSS feed: {feed_url}")
            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(JangNewsKarachiRSSPipeline.HEADERS),
            )
            try:
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            if not items:
                logger.warning(f"No items found in feed: {feed_url}")
                return []

            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    desc_elem = item.find("description")
                    creator_elem = item.find("dc:creator")
                    author_elem = item.find("author")
                    content_encoded_elem = item.find("content:encoded")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                    ]
                    if not categories:
                        categories = ["کراچی", "Karachi", "Pakistan"]

                    pub_date = pub_date_elem.get_text(strip=True) if pub_date_elem else ""

                    # Author extraction
                    raw_author = (
                        creator_elem.get_text(strip=True)
                        if creator_elem
                        else (author_elem.get_text(strip=True) if author_elem else "")
                    )
                    author = JangNewsKarachiRSSPipeline.clean_author(
                        raw_author, default="روزنامہ جنگ کراچی"
                    )

                    # 1. Fetch full article description using article URL
                    content = JangNewsKarachiRSSPipeline.full_description(link)

                    # 2. Fallback to RSS description if page scrape was empty
                    if not content:
                        fallback_html = (
                            content_encoded_elem.get_text()
                            if content_encoded_elem
                            else (desc_elem.get_text() if desc_elem else "")
                        )
                        content = JangNewsKarachiRSSPipeline.clean_content(fallback_html)

                    if len(content) < 200:
                        logger.info(f"Skipping article '{title}' (content length < 200 chars)")
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": JangNewsKarachiRSSPipeline.parse_date(pub_date),
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "ur",
                        "source": JangNewsKarachiRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": categories[:10],
                    })

                except Exception as e:
                    logger.warning(f"Failed to process Jang News item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Jang News Karachi articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Jang RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        try:
            logger.info("Starting Jang News Karachi RSS pipeline (concurrent)")
            all_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(JangNewsKarachiRSSPipeline.fetch_jang_rss_feed, feed): feed
                    for feed in JangNewsKarachiRSSPipeline.RSS_FEEDS
                }

                for future in concurrent.futures.as_completed(futures):
                    try:
                        all_articles.extend(future.result())
                    except Exception:
                        logger.exception("Feed processing failed")

            return all_articles

        except Exception as e:
            logger.error(f"Jang News Karachi pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = JangNewsKarachiRSSPipeline.process_input()

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            # Database insertion into Supabase with category="karachi"
            return SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

        except Exception as e:
            logger.error(f"Jang News Karachi RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }