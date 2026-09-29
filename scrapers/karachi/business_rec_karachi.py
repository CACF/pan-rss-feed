from datetime import datetime, timedelta, timezone
import logging
import re
import time
import urllib3
import uuid
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from app.utils.supabase_client import SupabaseClient
from config import KARACHI_TABLE

logger = logging.getLogger(__name__)


class BusinessRecorderKarachiRSSPipeline:

    SOURCE = "Business Recorder"
    RSS_FEEDS = [
        "https://www.brecorder.com/feeds/pakistan",  # Pakistan & Karachi News
    ]

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def create_driver(headless=True):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=3")
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        return driver

    @staticmethod
    def clean_author(author_str, default="BR Karachi Desk"):
        """
        Cleans RSS author format:
        Extracts the name from inside parentheses and removes dummy email addresses.
        e.g. 'none@none.com (Muhammad Saleem)' -> 'Muhammad Saleem'
        """
        if not author_str:
            return default

        # 1. Extract name from inside parentheses
        match = re.search(r"\((.*?)\)", author_str)
        if match and match.group(1).strip():
            return match.group(1).strip()

        # 2. Strip email addresses if no parentheses exist
        cleaned = re.sub(r"[\w\.-]+@[\w\.-]+", "", author_str).strip()
        cleaned = cleaned.strip("()[] -:")

        return cleaned if cleaned else default

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
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
        """Fetch RSS items using requests with SSL verify disabled."""
        try:
            logger.info(f"Fetching RSS feed: {feed_url}")
            response = requests.get(
                feed_url,
                headers=BusinessRecorderKarachiRSSPipeline.HEADERS,
                timeout=15,
                verify=False,
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
        """
        Extract full story content:
        Attempts direct HTTP request first, falls back to Selenium if needed.
        """
        try:
            # 1. Fast direct HTTP request attempt
            response = requests.get(
                link,
                headers=BusinessRecorderKarachiRSSPipeline.HEADERS,
                timeout=15,
                verify=False,
            )
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                content_div = soup.find("div", class_="story__content")
                if content_div:
                    return BusinessRecorderKarachiRSSPipeline.clean_content(str(content_div))

            # 2. Selenium fallback
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

            return BusinessRecorderKarachiRSSPipeline.clean_content(content_html)

        except Exception as e:
            logger.warning(f"Failed to fetch article content for {link}: {e}")
            return ""

    @staticmethod
    def process_feed(feed_url):
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        feed_build_date = datetime.now(timezone.utc)
        articles = []

        items = BusinessRecorderKarachiRSSPipeline.fetch_rss_items(feed_url)
        if not items:
            return []

        driver = BusinessRecorderKarachiRSSPipeline.create_driver(headless=True)

        try:
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
                    category = category_elem.get_text(strip=True) if category_elem else "Karachi"

                    # Clean author to remove 'none@none.com' and extract clean name
                    raw_author = author_elem.get_text(strip=True) if author_elem else ""
                    author = BusinessRecorderKarachiRSSPipeline.clean_author(
                        raw_author, default="BR Karachi Desk"
                    )

                    article_pub_date = BusinessRecorderKarachiRSSPipeline.parse_date(pub_date)
                    if article_pub_date < seven_days_ago:
                        continue

                    content = BusinessRecorderKarachiRSSPipeline.fetch_article_content(
                        driver, link, content_encoded_elem, desc_elem
                    )

                    if not content or len(content) < 200:
                        logger.info(f"Skipping short article: '{title}' (length: {len(content)})")
                        continue

                    articles.append({
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": BusinessRecorderKarachiRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Karachi",
                        "media_origin": "local",
                        "tags": [category, "Karachi", "Pakistan"],
                    })

                except Exception as e:
                    logger.warning(f"Failed to process article {link}: {e}")
                    continue
        finally:
            driver.quit()

        logger.info(f"Processed {len(articles)} Business Recorder Karachi articles.")
        return articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or KARACHI_TABLE
            all_articles = []

            for feed_url in BusinessRecorderKarachiRSSPipeline.RSS_FEEDS:
                articles = BusinessRecorderKarachiRSSPipeline.process_feed(feed_url)
                all_articles.extend(articles)

            # Deduplicate by id (link)
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            # Database insertion into Supabase with category="karachi"
            return SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="karachi"
            )

        except Exception as e:
            logger.error(f"Business Recorder Karachi pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }