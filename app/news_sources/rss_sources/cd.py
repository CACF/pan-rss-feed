import uuid
import logging
from datetime import datetime, timezone
import time
import concurrent.futures
from bs4 import BeautifulSoup
import re
import requests

from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class CoinDeskRSSPipeline:
    """
    CoinDesk RSS feed pipeline that fetches, parses,
    and stores full CoinDesk articles.
    """

    SOURCE = "CoinDesk"

    RSS_FEEDS = [
        "https://www.coindesk.com/arc/outboundfeeds/rss"
    ]

    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
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
    def clean_text(html):
        if not html:
            return ""

        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "aside", "figure"]):
            tag.decompose()

        text = soup.get_text(separator=" ")
        text = re.sub(r"http\S+|www\.\S+", "", text)
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    # ------------------------------------------------------------------
    # Full article fetch
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_full_article(link):
        """
        Fetch full CoinDesk article body + author
        """
        content = []
        authors = []

        try:
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(CoinDeskRSSPipeline.headers)
            )

            if res.status_code != 200:
                return None, None

            soup = BeautifulSoup(res.content, "lxml")

            # -------- Author(s) --------
            for a in soup.select("div.font-sans.text-default.text-sm > a[href^='/author/']:first-of-type"):
                name = a.get_text(strip=True)
                if name:
                    authors.append(name)

            # -------- Article body --------
            paragraphs = soup.select("div[data-module-name='article-body'] p")

            for p in paragraphs:
                text = p.get_text(strip=True)
                text = re.sub(r"http\S+|www\.\S+", "", text)
                if text:
                    content.append(text)

            full_text = " ".join(content) if content else None
            author_text = ", ".join(authors) if authors else "Unknown"

            return full_text, author_text

        except Exception as e:
            logger.warning(f"Failed to fetch full article: {e}")
            return None, None

        finally:
            try:
                res.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # RSS Fetch
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching CoinDesk RSS: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(CoinDeskRSSPipeline.headers)
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "lxml-xml")
            items = soup.find_all("item")

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title = item.find("title").get_text(strip=True)
                    link = item.find("link").get_text(strip=True)
                    pub_date = item.find("pubDate")

                    article_pub_date = (
                        CoinDeskRSSPipeline.parse_date(pub_date.get_text())
                        if pub_date else datetime.now(timezone.utc)
                    )

                    # Fetch full description
                    full_description, author = CoinDeskRSSPipeline.fetch_full_article(link)

                    if not full_description:
                        continue

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": CoinDeskRSSPipeline.SOURCE,
                        "content": full_description,
                        "genre": "Crypto",
                        "media_origin": "foreign",
                        "tags": "",
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process CoinDesk item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} CoinDesk articles")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch CoinDesk RSS: {e}")
            return []

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    @staticmethod
    def process_input(input_data=None):
        all_articles = []

        max_workers = 6

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(CoinDeskRSSPipeline.fetch_rss_feed, feed)
                for feed in CoinDeskRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception as e:
                    logger.exception(f"Feed failed: {e}")

        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):
        t0 = time.perf_counter()

        articles = CoinDeskRSSPipeline.process_input(input_data)

        if not articles:
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "elapsed_time": round(time.perf_counter() - t0, 2),
            }

        result = MongoDBClient.insert_articles_to_mongo(
            articles,
            user_email=input_data.get("email") if input_data else None
        )

        result["elapsed_time"] = round(time.perf_counter() - t0, 2)
        return result
