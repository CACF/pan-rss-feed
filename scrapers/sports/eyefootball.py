from config import SPORTS_TABLE
import uuid
import logging
import time
import random
import re
from datetime import datetime, timezone, timedelta

import cloudscraper
from bs4 import BeautifulSoup

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class EyefootballRSSPipeline:
    """
    Eyefootball RSS Pipeline
    """

    SOURCE = "Eyefootball"

    FEEDS = [
        "https://www.eyefootball.com/football_news.xml",
    ]

    # -----------------------------
    # HEADERS
    # -----------------------------
    @staticmethod
    def get_headers():
        return {
            "User-Agent": random.choice(
                [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                ]
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
        }

    # -----------------------------
    # DATE PARSER
    # -----------------------------
    @staticmethod
    def parse_date(date_str):
        try:
            if not date_str:
                return None
            return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            return None

    # -----------------------------
    # 7 DAY FILTER
    # -----------------------------
    @staticmethod
    def is_within_7_days(dt):
        if not dt:
            return False

        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=5)

        return seven_days_ago <= dt <= now

    @staticmethod
    def extract_author_from_html(soup):
        try:
            container = soup.select_one("p.articletimestamp a[rel='author']")
            if not container:
                return None

            name = container.get_text(strip=True)
            href = container.get("href")

            return {"name": name, "url": href}

        except Exception:
            return None

    # -----------------------------
    # CLEAN CONTENT
    # -----------------------------
    @staticmethod
    def clean_content(text):
        if not text:
            return text

        text = re.sub(r"\s+", " ", text)

        # remove ads / junk patterns
        patterns = [
            r"Advertisement",
            r"Google Ads?",
            r"transfer window",
        ]

        for p in patterns:
            text = re.sub(p, "", text, flags=re.IGNORECASE)

        return text.strip()

    # -----------------------------
    # ARTICLE SCRAPER
    # -----------------------------
    @staticmethod
    def fetch_article(scraper, url):
        try:
            response = scraper.get(
                url,
                headers=EyefootballRSSPipeline.get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            body = soup.find("span", itemprop="articleBody")
            if not body:
                return None

            # remove junk nodes
            for tag in body.find_all(["script", "style", "ins", "iframe", "noscript"]):
                tag.decompose()

            text = body.get_text(separator=" ", strip=True)
            text = EyefootballRSSPipeline.clean_content(text)
            html_author = EyefootballRSSPipeline.extract_author_from_html(soup)
            authors_str = html_author["name"] if html_author else "Eyefootball Staff"

            title_tag = soup.find("h1") or soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else ""

            return {
                "title": title,
                "content": text,
                "authors": authors_str,
            }

        except Exception as e:
            logger.warning(f"Article fetch failed: {url} | {e}")
            return None

    # -----------------------------
    # FEED PARSER
    # -----------------------------
    @staticmethod
    def fetch_feed(feed_url):
        try:
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )

            response = scraper.get(
                feed_url,
                headers=EyefootballRSSPipeline.get_headers(),
                timeout=30,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "xml")

            items = soup.find_all("item")
            articles = []

            feed_build_date = datetime.now(timezone.utc)

            for item in items:
                try:
                    title = item.find("title").text if item.find("title") else None
                    link = item.find("link").text if item.find("link") else None
                    pub_date = EyefootballRSSPipeline.parse_date(
                        item.find("pubDate").text if item.find("pubDate") else ""
                    )

                    if not link or not EyefootballRSSPipeline.is_within_7_days(
                        pub_date
                    ):
                        continue

                    full_article = EyefootballRSSPipeline.fetch_article(scraper, link)
                    if not full_article:
                        continue

                    content = full_article.get("content", "")
                    if len(content) < 200:
                        continue

                    author = full_article.get("authors")

                    articles.append(
                        {
                            "id": link,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": full_article.get("title") or title,
                            "authors": author,
                            "language": "en-US",
                            "image": None,
                            "source": EyefootballRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Sports",
                            "media_origin": "international",
                            "tags": [],
                            "url": link,
                        }
                    )

                except Exception as e:
                    logger.warning(f"Item parse failed: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Eyefootball articles")
            return articles

        except Exception as e:
            logger.error(f"Feed fetch failed: {e}")
            return []

    # -----------------------------
    # RUNNER
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = SPORTS_TABLE
            all_articles = []

            for feed in EyefootballRSSPipeline.FEEDS:
                all_articles.extend(EyefootballRSSPipeline.fetch_feed(feed))

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles(all_articles, table_name=target_table, category="sports")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
