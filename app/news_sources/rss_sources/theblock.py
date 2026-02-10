import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re
from curl_cffi import requests  # <--- use curl_cffi for Cloudflare bypass
from app.utilities import MongoDBClient

logger = logging.getLogger(__name__)


class TheBlockRSSPipeline:
    """
    The Block RSS feed pipeline.
    RSS -> link only
    Scrape content + author from article page (Cloudflare-safe)
    """

    SOURCE = "The Block"
    RSS_FEEDS = ["https://www.theblock.co/rss.xml"]

    BASE_HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    # -------------------
    # Session (Cloudflare-safe)
    # -------------------
    session = requests.Session(impersonate="chrome120")
    session.headers.update(BASE_HEADERS)

    # ---------- Date Parsing ----------
    @staticmethod
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S %z",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    # ---------- Text Cleaning ----------
    @staticmethod
    def clean_content(text):
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    # ---------- Full Article Scrape ----------
    @classmethod
    def full_description(cls, link):
        """
        Fetch full article content + author (Cloudflare-safe)
        """
        content = None
        author = "Unknown"
        paragraphs = []

        if not link:
            return None, author

        clean_link = link.split("?")[0]
        amp_link = clean_link.rstrip("/") + "/amp"

        try:
            # Cloudflare bypass request
            res = cls.session.get(amp_link, timeout=25)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "lxml")

            # ---- Content ----
            for p in soup.select(".dynamic-content > p"):
                text = p.get_text(strip=True)
                if len(text) < 40:
                    continue
                if any(x in text for x in ("©", "The Block", "All rights reserved")):
                    continue
                paragraphs.append(cls.clean_content(text))

            if paragraphs:
                content = " ".join(paragraphs)

            # ---- Author ----
            author_elem = soup.select_one("div.bylines a")
            if author_elem:
                author = author_elem.get_text(strip=True)

        except Exception as e:
            logger.warning(f"Article scrape failed {amp_link}: {e}")

        return content, author

    # ---------- RSS Fetch ----------
    @classmethod
    def fetch_theblock_rss_feed(cls, feed_url):
        logger.info(f"Fetching The Block RSS feed: {feed_url}")

        try:
            res = cls.session.get(feed_url, timeout=30)
            res.raise_for_status()
        except Exception as e:
            logger.error(f"RSS fetch failed {feed_url}: {e}")
            return []

        soup = BeautifulSoup(res.content, "lxml-xml")
        items = soup.find_all("item")
        feed_build_date = datetime.now(timezone.utc)

        articles = []

        for item in items:
            try:
                title = item.find("title").get_text(strip=True)
                link = item.find("link").get_text(strip=True)
                pub_date = item.find("pubDate")
                article_pub_date = (
                    cls.parse_date(pub_date.get_text()) if pub_date else feed_build_date
                )

                content, author = cls.full_description(link)
                if not content:
                    continue

                articles.append({
                    "_id": link,
                    "article_id": str(uuid.uuid4()),
                    "articlePubDate": article_pub_date,
                    "feedBuildDate": feed_build_date,
                    "title": title,
                    "authors": author,
                    "language": "en-us",
                    "source": cls.SOURCE,
                    "content": content,
                    "genre": "Crypto",
                    "media_origin": "foreign",
                    "tags": "",
                })

            except Exception as e:
                logger.warning(f"Item failed: {e}")

        logger.info(f"Parsed {len(articles)} articles from {feed_url}")
        return articles

    # ---------- Concurrent ----------
    @classmethod
    def process_input(cls, input_data=None):
        logger.info("Starting The Block RSS pipeline (concurrent)")
        all_articles = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(cls.fetch_theblock_rss_feed, feed)
                for feed in cls.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Feed processing failed")

        return all_articles

    # ---------- Run ----------
    @classmethod
    def run_pipeline(cls, input_data=None):
        start = time.perf_counter()
        articles = cls.process_input(input_data)

        if not articles:
            return {"inserted_count": 0, "total_articles": 0}

        result = MongoDBClient.insert_articles_to_mongo(
            articles,
            user_email=input_data.get("email") if input_data else None,
        )

        result["elapsed_time"] = round(time.perf_counter() - start, 2)
        logger.info(f"The Block pipeline finished in {result['elapsed_time']}s")
        return result
