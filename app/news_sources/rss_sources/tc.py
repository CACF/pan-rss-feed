import uuid
import logging
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import re
import requests
from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)

class TradeChronicleRSSPipeline:

    SOURCE = "Trade Chronicle"
    RSS_FEEDS = ["https://tradechronicle.com/feed/"]
    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }
    MAX_WORKERS = 5

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate to datetime UTC."""
        formats = ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Clean HTML to plain text."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure", "form"]):
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
        Fetch full article content from TradeChronicle article page.
        CSS selector: div.entry-content.clr p span
        """
        full_content = None
        author = "Trade Chronicle Desk"

        if not link:
            return full_content, author

        try:
            res = requests.get(link, timeout=30, headers=get_random_headers(TradeChronicleRSSPipeline.HEADERS))
            try:
                if res.status_code != 200:
                    return full_content, author

                soup = BeautifulSoup(res.content, "lxml")
                paragraphs = soup.select("div.entry-content.clr p span")
                texts = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if not texts:
                    paragraphs = soup.select("div.entry-content.clr p")
                    texts = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]

                if texts:
                    full_content = " ".join(texts)
                author_elem = soup.select_one("span.author-name")
                if author_elem:
                    author = author_elem.get_text(strip=True)

            finally:
                res.close()
        except Exception as e:
            logger.warning(f"Failed to fetch article {link}: {e}")

        return full_content, author

    @staticmethod
    def fetch_rss_feed(feed_url):
        """Fetch RSS feed and retrieve all full articles concurrently."""
        try:
            logger.info(f"Fetching RSS feed: {feed_url}")
            res = requests.get(feed_url, timeout=30, headers=get_random_headers(TradeChronicleRSSPipeline.HEADERS))
            try:
                res.raise_for_status()
                payload = res.content
            finally:
                res.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)
            articles = []

            def process_item(item):
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    category_elems = item.find_all("category")

                    if not title_elem or not link_elem:
                        return None

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    article_pub_date = TradeChronicleRSSPipeline.parse_date(pub_date_elem.get_text()) if pub_date_elem else datetime.now(timezone.utc)
                    content, author = TradeChronicleRSSPipeline.full_description(link)
                    if not content:
                        return None 

                    tags = [cat.get_text(strip=True) for cat in category_elems if cat.get_text(strip=True)]

                    return {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "source": TradeChronicleRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "foreign",
                        "tags": tags,
                    }
                except Exception as e:
                    logger.warning(f"Failed to process RSS item: {e}")
                    return None
            items = items[:25]

            with concurrent.futures.ThreadPoolExecutor(max_workers=TradeChronicleRSSPipeline.MAX_WORKERS) as executor:
                futures = [executor.submit(process_item, item) for item in items]
                for future in concurrent.futures.as_completed(futures):
                    article = future.result()
                    if article:
                        articles.append(article)

            logger.info(f"Fetched {len(articles)} full articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        """Fetch all feeds and insert into MongoDB."""
        all_articles = []
        for feed_url in TradeChronicleRSSPipeline.RSS_FEEDS:
            all_articles.extend(TradeChronicleRSSPipeline.fetch_rss_feed(feed_url))

        if not all_articles:
            return {"inserted_count": 0, "total_articles": 0}

        result = MongoDBClient.insert_articles_to_mongo(
            all_articles,
            user_email=input_data.get("email") if input_data else None
        )
        return result
