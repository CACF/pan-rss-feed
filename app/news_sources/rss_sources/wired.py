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


class WiredBusinessRSSPipeline:

    SOURCE = "WIRED"

    RSS_FEEDS = [
        "https://www.wired.com/feed/category/business/latest/rss/",
    ]

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

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate into datetime object."""
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

    @staticmethod
    def clean_content(text):
        if not text:
            return ""
        text = re.sub(r"http\S+|www\.\S+", "", text)
        return " ".join(text.split())

    @staticmethod
    def full_description(link):
        """
        Fetch full WIRED article content + author from article page.
        """

        content = None
        author = "WIRED Staff"
        tags = []
        image_url = None

        if not link:
            return None, author, tags, image_url

        try:
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(WiredBusinessRSSPipeline.BASE_HEADERS),
            )
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "lxml")

            paragraphs = []
            for p in soup.select("div.body__inner-container > p"):
                text = p.get_text(strip=True)
                if len(text) < 30:
                    continue
                paragraphs.append(
                    WiredBusinessRSSPipeline.clean_content(text)
                )

            if paragraphs:
                content = " ".join(paragraphs)

            author_elem = soup.select_one('span[itemprop="name"] a.byline__name-link')
            if author_elem:
                author = author_elem.get_text(strip=True)

            for tag in soup.select("a[href*='/tag/']"):
                tag_text = tag.get_text(strip=True)
                if tag_text and tag_text not in tags:
                    tags.append(tag_text)

            img_elem = soup.select_one("meta[property='og:image']")
            if img_elem and img_elem.get("content"):
                image_url = img_elem["content"]

        except Exception as e:
            logger.warning(f"Failed to fetch WIRED article {link}: {e}")

        return content, author, tags, image_url

    @staticmethod
    def fetch_wired_rss_feed(feed_url):

        try:
            logger.info(f"Fetching WIRED RSS feed: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(WiredBusinessRSSPipeline.BASE_HEADERS),
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

                    pub_date_elem = item.find("pubDate")
                    article_pub_date = (
                        WiredBusinessRSSPipeline.parse_date(
                            pub_date_elem.get_text()
                        )
                        if pub_date_elem
                        else feed_build_date
                    )

                    content, author, tags, image_url = (
                        WiredBusinessRSSPipeline.full_description(link)
                    )

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
                        "source": WiredBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "foreign",
                        "tags": tags,
                        "image_url": image_url,
                    })

                except Exception as e:
                    logger.warning(f"Failed to process WIRED item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} WIRED articles.")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch WIRED RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):

        all_articles = []
        logger.info("Starting WIRED RSS pipeline (concurrent)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    WiredBusinessRSSPipeline.fetch_wired_rss_feed,
                    feed
                )
                for feed in WiredBusinessRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Failed to process WIRED feed")

        logger.info(f"Total WIRED articles processed: {len(all_articles)}")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):

        start = time.perf_counter()
        articles = WiredBusinessRSSPipeline.process_input(input_data)

        if not articles:
            return {"inserted_count": 0, "total_articles": 0}

        result = MongoDBClient.insert_articles_to_mongo(
            articles,
            user_email=input_data.get("email") if input_data else None,
        )

        result["elapsed_time"] = round(time.perf_counter() - start, 2)

        logger.info(f"WIRED pipeline finished in {result['elapsed_time']}s")
        return result
