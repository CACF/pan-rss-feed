import uuid
import logging
import time
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from app.utilities import MongoDBClient

logger = logging.getLogger(__name__)


class BusinessRecorderRSSPipeline:
    """
    Business Recorder RSS feed pipeline that fetches RSS feeds via Selenium,
    opens articles with Selenium, and stores the parsed articles.
    """

    SOURCE = "Business Recorder"
    RSS_FEEDS = [
        "https://www.brecorder.com/feeds/business",
    ]

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def create_driver(headless=True):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        return driver

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style", "aside", "figure", "iframe"]):
                tag.decompose()
            for a in soup.find_all("a"):
                a.unwrap()
            text = soup.get_text(separator=" ", strip=True)
            text = " ".join(word for word in text.split() if not word.startswith("http"))
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    # -----------------------------
    # Fetch RSS via Selenium
    # -----------------------------
    @staticmethod
    def fetch_br_rss_feed(feed_url):
        try:
            logger.info(f"Fetching RSS feed via Selenium: {feed_url}")
            driver = BusinessRecorderRSSPipeline.create_driver(headless=True)
            driver.get(feed_url)
            time.sleep(2)  # allow XML to load

            soup = BeautifulSoup(driver.page_source, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)
            driver.quit()

            if not items:
                logger.warning(f"No items found in feed: {feed_url}")
                return []

            articles = []

            # Reuse one driver for all articles to save resources
            article_driver = BusinessRecorderRSSPipeline.create_driver(headless=True)

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    category_elem = item.find("category")
                    author_elem = item.find("author")
                    content_encoded_elem = item.find("content:encoded")
                    desc_elem = item.find("description")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    pub_date = pub_date_elem.get_text(strip=True) if pub_date_elem else ""
                    category = category_elem.get_text(strip=True) if category_elem else "Business"
                    author = author_elem.get_text(strip=True) if author_elem else "BR Web Desk"

                    # Open article in Selenium
                    article_driver.get(link)
                    # time.sleep(1)
                    article_soup = BeautifulSoup(article_driver.page_source, "html.parser")
                    content_div = article_soup.find("div", class_="story__content")

                    # Prefer content:encoded, fallback to article div, then description
                    if content_encoded_elem:
                        content_html = content_encoded_elem.get_text()
                    elif content_div:
                        content_html = str(content_div)
                    else:
                        content_html = desc_elem.get_text() if desc_elem else ""

                    content = BusinessRecorderRSSPipeline.clean_content(content_html)
                    if not content or len(content) < 200:
                        logger.info(f"Skipping short article: '{title}' (length: {len(content)})")
                        continue

                    article = {
                        "_id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": BusinessRecorderRSSPipeline.parse_date(pub_date),
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": BusinessRecorderRSSPipeline.SOURCE,
                        "content": content,
                        "genre": category,
                        "media_origin": "local",
                        "tags": [category],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process article {link}: {e}")
                    continue

            article_driver.quit()
            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"Selenium RSS fetch failed for {feed_url}: {e}")
            return []

    # -----------------------------
    # Process all feeds concurrently
    # -----------------------------
    @staticmethod
    def process_input(input_data=None):
        try:
            logger.info("Starting Business Recorder RSS pipeline (concurrent)")
            all_articles = []
            max_workers = 5

            def _fetch(feed):
                return BusinessRecorderRSSPipeline.fetch_br_rss_feed(feed)

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_feed = {
                    executor.submit(_fetch, feed): feed
                    for feed in BusinessRecorderRSSPipeline.RSS_FEEDS
                }

                for future in concurrent.futures.as_completed(future_to_feed):
                    feed = future_to_feed[future]
                    try:
                        articles = future.result()
                        all_articles.extend(articles)
                        logger.info(f"Feed processed: {feed} -> {len(articles)} articles")
                    except Exception as e:
                        logger.exception(f"Feed failed: {feed}")
                        continue

            logger.info(f"Business Recorder pipeline processed {len(all_articles)} total articles")
            return all_articles

        except Exception as e:
            logger.error(f"Business Recorder RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            logger.info("Starting Business Recorder RSS pipeline")
            t0 = time.perf_counter()
            articles = BusinessRecorderRSSPipeline.process_input(input_data)

            if not articles:
                elapsed = round(time.perf_counter() - t0, 2)
                logger.warning("No Business Recorder articles to insert")
                return {"inserted_count": 0, "total_articles": 0, "elapsed_time": elapsed}

            result = MongoDBClient.insert_articles_to_mongo(
                articles,
                user_email=input_data.get("email") if input_data else None,
            )
            result["elapsed_time"] = round(time.perf_counter() - t0, 2)
            logger.info(f"Business Recorder pipeline finished in {result['elapsed_time']}s")
            return result

        except Exception as e:
            elapsed = round(time.perf_counter(), 2)
            logger.error(f"Business Recorder RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
                "elapsed_time": elapsed,
            }
