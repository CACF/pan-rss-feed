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


class ForbesRSSPipeline:

    SOURCE = "Forbes"

    RSS_FEEDS = [
        "https://feeds.forbes.com/business/feed/",
        "https://feeds.forbes.com/money/feed/",
        "https://feeds.forbes.com/real-estate/feed/",
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
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).astimezone(timezone.utc)
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
        Fetch full Forbes article content using session + proper headers
        """

        if not link:
            return None

        try:
            session = requests.Session()

            headers = get_random_headers(ForbesRSSPipeline.HEADERS)
            headers.update({
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0",
            })

            # Step 1: First visit homepage (important for cookies)
            session.get("https://www.forbes.com/", headers=headers, timeout=30)

            # Small delay to look human
            time.sleep(1.5)

            # Step 2: Visit actual article
            response = session.get(link, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            paragraphs = []

            # Forbes sometimes changes class names
            selectors = [
               ".article-body-container p"
            ]

            for selector in selectors:
                for p in soup.select(selector):
                    text = p.get_text(strip=True)

                    if len(text) < 40:
                        continue

                    if any(x in text for x in ("Forbes", "©", "All Rights Reserved")):
                        continue

                    paragraphs.append(
                        ForbesRSSPipeline.clean_content(text)
                    )

                if paragraphs:
                    break  # stop if content found

            if paragraphs:
                return " ".join(paragraphs)

        except Exception as e:
            logger.warning(f"Failed to fetch full article {link}: {e}")

        return None

    @staticmethod
    def fetch_forbes_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Forbes RSS feed: {feed_url}")

            response = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(ForbesRSSPipeline.HEADERS),
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

                    pub_date_tag = item.find("pubDate")
                    article_pub_date = (
                        ForbesRSSPipeline.parse_date(pub_date_tag.get_text())
                        if pub_date_tag else feed_build_date
                    )

                    # Get author from RSS feed
                    rss_author = item.find("dc:creator")
                    if rss_author:
                        author = rss_author.get_text(strip=True)
                    else:
                        atom_author = item.find("atom:author")
                        author = (
                            atom_author.find("atom:name").get_text(strip=True)
                            if atom_author and atom_author.find("atom:name")
                            else "Unknown"
                        )

                    content = ForbesRSSPipeline.full_description(link)
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
                        "source": ForbesRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "foreign",
                        "tags": "",
                    })

                except Exception as e:
                    logger.warning(f"Failed to process item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        logger.info("Starting Forbes RSS pipeline")

        all_articles = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    ForbesRSSPipeline.fetch_forbes_rss_feed,
                    feed
                )
                for feed in ForbesRSSPipeline.RSS_FEEDS
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    logger.exception("Feed processing failed")

        logger.info(f"Total articles processed: {len(all_articles)}")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):
        start = time.perf_counter()

        articles = ForbesRSSPipeline.process_input(input_data)

        if not articles:
            return {"inserted_count": 0, "total_articles": 0}

        result = MongoDBClient.insert_articles_to_mongo(
            articles,
            user_email=input_data.get("email") if input_data else None,
        )

        result["elapsed_time"] = round(time.perf_counter() - start, 2)
        logger.info(f"Forbes pipeline finished in {result['elapsed_time']}s")

        return result