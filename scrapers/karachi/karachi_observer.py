from config import KARACHI_TABLE
import uuid
import logging
from datetime import datetime, timezone
import concurrent.futures
from bs4 import BeautifulSoup
from lxml import etree
import re
import requests
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class KarachiObserverRSSPipeline:

    SOURCE = "Karachi Observer"

    RSS_FEEDS = [
        "https://karachiobserver.com/feed/",
        "https://karachiobserver.com/category/karachi/feed/",
        "https://karachiobserver.com/category/karachi-crimes/feed/",
        "https://karachiobserver.com/category/karachi-politics/feed/",
        "https://karachiobserver.com/category/karachi-education/feed/",
        "https://karachiobserver.com/category/karachi-events/feed/",
        "https://karachiobserver.com/category/karachi-business/feed/",
        "https://karachiobserver.com/category/health-fitness/feed/",
    ]

    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    @staticmethod
    def parse_rss_date(date_str):
        """Parse RSS pubDate into UTC datetime."""
        try:
            return datetime.strptime(
                date_str, "%a, %d %b %Y %H:%M:%S %z"
            ).astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(text):
        """Clean text by removing URLs and extra spaces."""
        if not text:
            return ""
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @staticmethod
    def parse_rss_safely(payload):
        """
        Karachi Observer's feed occasionally has malformed XML (bad byte
        sequences in attributes/CDATA) that trips up strict lxml-xml
        parsing, causing BeautifulSoup to silently find 0 items. Parse
        with libxml2's recover=True first to salvage what's parseable,
        then hand the recovered tree to BeautifulSoup so the rest of the
        code (.find(), .find_all()) works unchanged.
        """
        try:
            parser = etree.XMLParser(recover=True, encoding="utf-8")
            tree = etree.fromstring(payload, parser=parser)
            recovered_xml = etree.tostring(tree, encoding="utf-8")
            return BeautifulSoup(recovered_xml, "lxml-xml")
        except Exception as e:
            logger.warning(f"Recovery parse failed, falling back to raw parse: {e}")
            return BeautifulSoup(payload, "lxml-xml")

    @staticmethod
    def full_description(link):
        """
        RSS description/content:encoded for this source is too short
        (often under 150 chars, just a teaser sentence), so fetch the
        actual article page and scrape the full body paragraphs.
        """
        if not link:
            return None

        try:
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(KarachiObserverRSSPipeline.HEADERS)
            )
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "lxml")

            paragraphs = []

            for p in soup.select("div.entry-content p"):

                text = p.get_text(strip=True)

                if len(text) < 40:
                    continue

                if any(x in text for x in (
                    "Karachi Observer",
                    "first appeared on",
                    "owns the property",
                    "Read Also",
                    "Also Read",
                )):
                    continue

                paragraphs.append(
                    KarachiObserverRSSPipeline.clean_content(text)
                )

            if paragraphs:
                return " ".join(paragraphs)

        except Exception as e:
            logger.warning(f"Failed to fetch Karachi Observer article {link}: {e}")

        return None

    @staticmethod
    def fetch_karachiobserver_feed(feed_url):
        """Fetch and parse a Karachi Observer RSS feed."""
        try:
            logger.info(f"Fetching Karachi Observer feed: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(KarachiObserverRSSPipeline.HEADERS)
            )
            response.raise_for_status()

            soup = KarachiObserverRSSPipeline.parse_rss_safely(response.content)

            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)

            if not items:
                logger.warning(f"No items found in feed after recovery parse: {feed_url}")
                return []

            articles = []

            for item in items:
                try:
                    title_tag = item.find("title")
                    link_tag = item.find("link")

                    if not title_tag or not link_tag:
                        continue

                    title = title_tag.get_text(strip=True)
                    link = link_tag.get_text(strip=True)

                    pub_date_tag = item.find("pubDate")
                    article_pub_date = (
                        KarachiObserverRSSPipeline.parse_rss_date(pub_date_tag.get_text())
                        if pub_date_tag else feed_build_date
                    )

                    author_tag = item.find("dc:creator")
                    author = author_tag.get_text(strip=True) if author_tag else "Karachi Observer Staff"

                    categories = [
                        cat.get_text(strip=True)
                        for cat in item.find_all("category")
                    ]

                    content = KarachiObserverRSSPipeline.full_description(link)

                    if not content:
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": KarachiObserverRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": categories,
                    })

                except Exception as e:
                    logger.warning(f"Failed to process Karachi Observer item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch Karachi Observer feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        """Process all Karachi Observer feeds concurrently."""
        all_articles = []
        logger.info("Starting Karachi Observer RSS pipeline")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(KarachiObserverRSSPipeline.fetch_karachiobserver_feed, feed)
                for feed in KarachiObserverRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Failed to process feed")

        logger.info(f"Total Karachi Observer articles processed: {len(all_articles)}")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = KarachiObserverRSSPipeline.process_input()

            # Deduplicate by id (link) — several category feeds overlap
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

            return result

        except Exception as e:
            logger.error(f"Karachi Observer RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }