from config import FASHION_TABLE
import re
import time
import uuid
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class SundayFashionRSSPipeline:
    """
    Sunday (Pakistan) Fashion RSS Pipeline

    The feed returns 403 Forbidden on a plain HTTP request (cloudscraper
    included), so this pipeline opens the feed URL with a real headless
    Chrome browser via Selenium first. That either:
      1) lets us read the raw XML straight from the rendered page, or
      2) at minimum solves whatever bot-check set the 403 and gives us
         valid cookies + a real User-Agent, which we then reuse in a
         plain `requests` call to fetch the clean XML for parsing.

    We try (1) first since it avoids an extra network round trip; if the
    page source doesn't look like a valid RSS/XML document (e.g. Chrome
    wrapped it in its built-in XML viewer chrome, or a challenge page is
    still showing), we fall back to (2).

    Author (dc:creator), datetime (pubDate), and content (content:encoded)
    are all present natively in the feed, so no article-page scraping is
    needed once we have the raw XML.
    """

    SOURCE = "Sunday"

    RSS_FEEDS = [
        "https://sunday.com.pk/feed/",
    ]

    SELENIUM_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    # -----------------------------
    # DATE PARSER
    # -----------------------------
    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return (
                    dt.astimezone(timezone.utc)
                    if dt.tzinfo
                    else dt.replace(tzinfo=timezone.utc)
                )
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return datetime.now(timezone.utc)

    # -----------------------------
    # CLEAN CONTENT
    # -----------------------------
    @staticmethod
    def clean_text(html):
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            # remove noise
            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()

            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    # -----------------------------
    # FASHION FILTER
    # -----------------------------
    @staticmethod
    def is_fashion_related(title, categories):
        keywords = [
            "fashion",
            "style",
            "bridal",
            "outfit",
            "designer",
            "collection",
            "trend",
            "runway",
            "beauty",
            "makeup",
            "wedding",
            "couture",
            "clothing",
            "jewellery",
            "jewelry",
        ]

        title_lower = title.lower()

        if any(k in title_lower for k in keywords):
            return True

        # Sunday uses categories like Fashion, Beauty, Lifestyle, Entertainment, etc.
        allowed_categories = {"fashion", "beauty", "lifestyle"}

        for cat in categories:
            if cat.lower() in allowed_categories:
                return True

        return False

    # -----------------------------
    # SELENIUM DRIVER
    # -----------------------------
    @staticmethod
    def _build_driver():
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"user-agent={SundayFashionRSSPipeline.SELENIUM_USER_AGENT}")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        return webdriver.Chrome(options=options)

    # -----------------------------
    # OPEN FEED WITH SELENIUM, RETURN RAW XML TEXT
    # -----------------------------
    @staticmethod
    def _fetch_via_selenium(feed_url):
        driver = None
        try:
            driver = SundayFashionRSSPipeline._build_driver()
            driver.get(feed_url)
            time.sleep(2)  # let any JS challenge / redirect settle

            page_source = driver.page_source

            # If Chrome rendered a proper XML document, the raw <rss ...>
            # tag is usually still present somewhere in page_source, even
            # if wrapped in Chrome's built-in XML viewer HTML. Pull it out
            # with a regex instead of trusting page_source structure.
            match = re.search(r"<\?xml.*?</rss\s*>", page_source, re.DOTALL)
            if match:
                return match.group(0)

            # Fallback: maybe it rendered without the <?xml ...?> prolog
            match = re.search(r"<rss[^>]*>.*</rss\s*>", page_source, re.DOTALL)
            if match:
                return match.group(0)

            # Couldn't find raw XML in the page source (likely wrapped in
            # Chrome's XML viewer shadow DOM) -> fall back to cookie
            # passthrough with requests instead.
            cookies = driver.get_cookies()
            user_agent = driver.execute_script("return navigator.userAgent;")
            return SundayFashionRSSPipeline._fetch_via_cookies(
                feed_url, cookies, user_agent
            )

        except Exception as e:
            logger.error(f"Selenium fetch failed for {feed_url}: {e}")
            return None
        finally:
            if driver is not None:
                driver.quit()

    # -----------------------------
    # FALLBACK: REUSE SELENIUM COOKIES IN A PLAIN REQUEST
    # -----------------------------
    @staticmethod
    def _fetch_via_cookies(feed_url, selenium_cookies, user_agent):
        try:
            session = requests.Session()
            for cookie in selenium_cookies:
                session.cookies.set(cookie["name"], cookie["value"])

            headers = get_random_headers()
            headers["User-Agent"] = user_agent or SundayFashionRSSPipeline.SELENIUM_USER_AGENT

            response = session.get(feed_url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text

        except Exception as e:
            logger.error(f"Cookie-passthrough fetch failed for {feed_url}: {e}")
            return None

    # -----------------------------
    # FETCH RSS
    # -----------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Sunday RSS via Selenium: {feed_url}")

            raw_xml = SundayFashionRSSPipeline._fetch_via_selenium(feed_url)
            if not raw_xml:
                logger.error(f"Could not retrieve XML for {feed_url}")
                return []

            soup = BeautifulSoup(raw_xml, "lxml-xml")
            items = soup.find_all("item")

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title = (
                        item.find("title").get_text(strip=True)
                        if item.find("title")
                        else None
                    )
                    link = (
                        item.find("link").get_text(strip=True)
                        if item.find("link")
                        else None
                    )

                    if not title or not link:
                        continue

                    categories = [
                        c.get_text(strip=True) for c in item.find_all("category") if c
                    ]

                    pub_date = (
                        SundayFashionRSSPipeline.parse_date(
                            item.find("pubDate").get_text()
                        )
                        if item.find("pubDate")
                        else datetime.now(timezone.utc)
                    )

                    content_raw = (
                        item.find("content:encoded").get_text()
                        if item.find("content:encoded")
                        else (
                            item.find("description").get_text()
                            if item.find("description")
                            else ""
                        )
                    )

                    content = SundayFashionRSSPipeline.clean_text(content_raw)

                    author = (
                        item.find("dc:creator").get_text(strip=True)
                        if item.find("dc:creator")
                        else "Sunday"
                    )

                    # skip short articles
                    if len(content) < 200:
                        continue

                    # fashion filter
                    if not SundayFashionRSSPipeline.is_fashion_related(
                        title, categories
                    ):
                        continue

                    articles.append(
                        {
                            "id": link,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "authors": author,
                            "language": "en-US",
                            "image": None,
                            "source": SundayFashionRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Fashion",
                            "media_origin": "local",
                            "tags": categories,
                        }
                    )

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Sunday articles.")
            return articles

        except Exception as e:
            logger.error(f"Sunday RSS fetch failed: {e}")
            return []

    # -----------------------------
    # RUN PIPELINE
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = FASHION_TABLE
            all_articles = []

            for feed_url in SundayFashionRSSPipeline.RSS_FEEDS:
                all_articles.extend(
                    SundayFashionRSSPipeline.fetch_rss_feed(feed_url)
                )

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles_current_year(
                all_articles, table_name=target_table
            )

        except Exception as e:
            logger.error(f"Sunday pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }