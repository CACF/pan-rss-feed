from config import BUSINESS_TABLE
import uuid
import logging
import time
import concurrent.futures
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import requests

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class BusinessRecorderRSSPipeline:

    SOURCE = "Business Recorder"
    RSS_FEEDS = [
        "https://www.brecorder.com/feeds/business",
    ]

    @staticmethod
    def create_driver(headless=True):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
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

    @staticmethod
    def fetch_rss_items(feed_url):
        """Fetch RSS items using requests instead of Selenium to avoid SSL issues."""
        try:
            logger.info(f"Fetching RSS feed: {feed_url}")
            response = requests.get(
                feed_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
                verify=False
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml-xml")
            items = soup.find_all("item")
            if not items:
                logger.warning(f"No items found in feed: {feed_url}")
            return items
        except Exception as e:
            logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def fetch_article_content(driver, link, content_encoded_elem=None, desc_elem=None):
        """Scrape article content using Selenium."""
        try:
            driver.get(link)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            content_div = soup.find("div", class_="story__content")
            if content_encoded_elem:
                content_html = content_encoded_elem.get_text()
            elif content_div:
                content_html = str(content_div)
            elif desc_elem:
                content_html = desc_elem.get_text()
            else:
                content_html = ""
            return BusinessRecorderRSSPipeline.clean_content(content_html)
        except Exception as e:
            logger.warning(f"Failed to fetch article content {link}: {e}")
            return ""

    @staticmethod
    def process_feed(feed_url):
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        feed_build_date = datetime.now(timezone.utc)
        articles = []

        items = BusinessRecorderRSSPipeline.fetch_rss_items(feed_url)
        if not items:
            return []

        driver = BusinessRecorderRSSPipeline.create_driver(headless=True)

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

                article_pub_date = BusinessRecorderRSSPipeline.parse_date(pub_date)
                if article_pub_date < seven_days_ago:
                    continue

                content = BusinessRecorderRSSPipeline.fetch_article_content(
                    driver, link, content_encoded_elem, desc_elem
                )

                if not content or len(content) < 200:
                    logger.info(f"Skipping short article: '{title}' (length: {len(content)})")
                    continue

                article = {
                    "id": link,
                    "article_id": str(uuid.uuid4()),
                    "articlePubDate": article_pub_date,
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

        driver.quit()
        logger.info(f"Processed {len(articles)} articles from feed {feed_url}")
        return articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or BUSINESS_TABLE
            all_articles = []

            for feed_url in BusinessRecorderRSSPipeline.RSS_FEEDS:
                articles = BusinessRecorderRSSPipeline.process_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"After dedupe: {len(all_articles)} articles"
            )

            result = SupabaseClient.insert_articles(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"Business Recorder pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
