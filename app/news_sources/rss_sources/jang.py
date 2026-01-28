import uuid
import logging
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import re
import requests
from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)


class JangBusinessRSSPipeline:
    """
    Jang Business RSS pipeline:
    - Fetch RSS feed
    - Open each news link
    - Extract full article text using CSS selector
    - Store in MongoDB
    """

    SOURCE = "Daily Jang"
    RSS_FEEDS = ["https://jang.com.pk/rss/1/3"]
    LANGUAGE = "ur"
    GENRE = "Business"

    HEADERS = {
        "Accept-Language": "ur-PK,ur;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    MAX_WORKERS = 5

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to UTC datetime."""
        try:
            dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z")
            return dt.astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_text(text):
        """Normalize Urdu text."""
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def full_description(link):
        """
        Fetch full article content from Jang news page.
        Primary CSS selector:
        - div#news-main-text p
        """
        if not link:
            return None, "Jang Desk"

        try:
            res = requests.get(
                link,
                timeout=30,
                headers=get_random_headers(JangBusinessRSSPipeline.HEADERS)
            )

            if res.status_code != 200:
                return None, "Jang Desk"

            soup = BeautifulSoup(res.content, "lxml")

            # ✅ Main article paragraphs
            paragraphs = soup.select("div.detail_view_content > p")

            texts = [
                p.get_text(strip=True)
                for p in paragraphs
                if p.get_text(strip=True) and p.get_text(strip=True) != "\xa0"
            ]

            if not texts:
                return None, "Jang Desk"

            content = JangBusinessRSSPipeline.clean_text(" ".join(texts))

            return content, "Jang Desk"

        except Exception as e:
            logger.warning(f"Failed to fetch article {link}: {e}")
            return None, "Jang Desk"

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch RSS and process items concurrently."""
        try:
            logger.info(f"Fetching RSS feed: {feed_url}")
            res = requests.get(
                feed_url,
                timeout=30,
                headers=get_random_headers(JangBusinessRSSPipeline.HEADERS)
            )
            res.raise_for_status()

            soup = BeautifulSoup(res.content, "lxml-xml")
            items = soup.find_all("item")[:25]
            feed_build_date = datetime.now(timezone.utc)

            articles = []

            def process_item(item):
                try:
                    title = item.find("title").get_text(strip=True)
                    link = item.find("link").get_text(strip=True)
                    pub_date = item.find("pubDate")
                    article_pub_date = (
                        JangBusinessRSSPipeline.parse_date(pub_date.get_text())
                        if pub_date else datetime.now(timezone.utc)
                    )

                    content, author = JangBusinessRSSPipeline.full_description(link)
                    if not content:
                        return None

                    return {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": JangBusinessRSSPipeline.LANGUAGE,
                        "source": JangBusinessRSSPipeline.SOURCE,
                        "content": content,
                        "genre": JangBusinessRSSPipeline.GENRE,
                        "media_origin": "local",
                        "tags": ["business", "economy", "finance"],
                    }

                except Exception as e:
                    logger.warning(f"Failed to process RSS item: {e}")
                    return None

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=JangBusinessRSSPipeline.MAX_WORKERS
            ) as executor:
                futures = [executor.submit(process_item, item) for item in items]
                for future in concurrent.futures.as_completed(futures):
                    article = future.result()
                    if article:
                        articles.append(article)

            logger.info(f"Fetched {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Run pipeline and insert into MongoDB."""
        all_articles = []

        for feed_url in JangBusinessRSSPipeline.RSS_FEEDS:
            all_articles.extend(
                JangBusinessRSSPipeline.fetch_rss_feed(feed_url)
            )

        if not all_articles:
            return {"inserted_count": 0, "total_articles": 0}

        return MongoDBClient.insert_articles_to_mongo(
            all_articles,
            user_email=input_data.get("email") if input_data else None
        )
