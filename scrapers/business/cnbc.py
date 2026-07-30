from config import BUSINESS_TABLE
import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class CNBCRSSPipeline:
    """
    CNBC RSS feed pipeline that fetches, parses, and stores CNBC news articles.
    """

    SOURCE = "CNBC"
    RSS_FEEDS = [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001054",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000116",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=44877279",
    ]
    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        """Parse RSS date format to datetime object."""
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Convert HTML (if present) to clean plain text and remove URLs."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()

            text = soup.get_text()
            # Remove any URL patterns
            text = re.sub(r"http\S+|www\.\S+", "", text)
            # Normalize spaces
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            return " ".join(chunk for chunk in chunks if chunk)
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    @staticmethod
    def full_description(link):
        """Fetch full article and remove any embedded links from content."""
        full_description = None
        author = None
        content_paragraphs = []

        if link:
            res1 = requests.get(link, timeout=30, headers=get_random_headers(CNBCRSSPipeline.headers))
            try:
                if res1.status_code == 200:
                    soup1 = BeautifulSoup(res1.content, "lxml")

                    # Author
                    author_elem = soup1.select_one('div.Author-author a.Author-authorName')
                    author = author_elem.get_text(strip=True) if author_elem else "Unknown"

                    # Full article paragraphs
                    paragraphs = soup1.select("div.ArticleBody-articleBody div.group p")
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        # Remove any URLs from content text
                        text = re.sub(r"http\S+|www\.\S+", "", text)
                        if text:
                            content_paragraphs.append(text)

                    if content_paragraphs:
                        full_description = " ".join(content_paragraphs)
            finally:
                res1.close()
        return full_description, author

    @staticmethod
    def fetch_cnbc_rss_feed(feed_url):
        """Fetch and parse a single CNBC RSS feed."""
        try:
            logger.info(f"Fetching CNBC RSS feed: {feed_url}")
            response = requests.get(feed_url, timeout=30, headers=get_random_headers(CNBCRSSPipeline.headers))
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
                    guid_elem = item.find("guid")
                    id_elem = item.find("metadata:id")
                    type_elem = item.find("metadata:type")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text().strip()
                    link = link_elem.get_text().strip()
                    description_text = desc_elem.get_text().strip() if desc_elem else ""
                    content = CNBCRSSPipeline.clean_content(description_text)

                    # Publication date
                    article_pub_date = CNBCRSSPipeline.parse_date(pub_date_elem.get_text())

                    full_description, author = CNBCRSSPipeline.full_description(link)
                    
                    if full_description:
                        description_text = full_description
                    elif item.find("description"):
                        description_text = content
                    else:
                        description_text = ""

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": CNBCRSSPipeline.SOURCE,
                        "content": description_text,
                        "genre":"Business",
                        "media_origin": "foreign",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process CNBC article: {e}")
                    continue

            logger.info(f"Successfully parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch CNBC RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all CNBC RSS feeds concurrently."""
        try:
            logger.info("Starting CNBC RSS pipeline processing (concurrent)")
            all_articles = []

            max_workers = 10

            def _fetch(feed_url: str):
                return CNBCRSSPipeline.fetch_cnbc_rss_feed(feed_url)

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_feed = {executor.submit(_fetch, feed): feed for feed in CNBCRSSPipeline.RSS_FEEDS}
                for future in concurrent.futures.as_completed(future_to_feed):
                    feed = future_to_feed[future]
                    try:
                        articles = future.result()
                        all_articles.extend(articles)
                        logger.info(f"Feed processed: {feed} -> {len(articles)} articles")
                    except Exception as e:
                        logger.exception(f"Feed failed: {feed}")
                        continue

            logger.info(f"CNBC RSS pipeline processed {len(all_articles)} total articles")
            return all_articles
        except Exception as e:
            logger.error(f"CNBC RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = BUSINESS_TABLE
            logger.info("Starting CNBC RSS pipeline")

            all_articles = CNBCRSSPipeline.process_input()

            # Deduplicate by article link
            all_articles = list(
                {
                    article["id"]: article
                    for article in all_articles
                }.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            if not all_articles:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                }

            result = SupabaseClient.insert_articles(
                all_articles
            , table_name=target_table)

            return result

        except Exception as e:
            logger.exception(
                f"CNBC RSS pipeline failed: {e}"
            )

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
