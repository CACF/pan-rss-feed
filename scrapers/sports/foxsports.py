from config import SPORTS_TABLE
import uuid
import json
import logging
import time
import random
import re
from datetime import datetime, timezone, timedelta

import cloudscraper
from bs4 import BeautifulSoup

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class FoxSportsRSSPipeline:
    """
    FOX Sports Sitemap Pipeline
    """

    SOURCE = "FOX Sports"

    FEEDS = [
        "https://www.foxsports.com/sitemap.xml?type=articles",
    ]

    # -----------------------------
    # HEADERS (ANTI-406 BASIC BYPASS)
    # -----------------------------
    @staticmethod
    def get_headers():
        return {
            "User-Agent": random.choice(
                [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                ]
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

    # -----------------------------
    # DATE PARSER
    # -----------------------------
    @staticmethod
    def parse_date(date_str):
        try:
            if not date_str:
                return None
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return None

    # -----------------------------
    # 7-DAY FILTER
    # -----------------------------
    @staticmethod
    def is_within_7_days(dt):
        if not dt:
            return False

        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=3)

        return seven_days_ago <= dt <= now

    # -----------------------------
    # CLEAN CONTENT (REMOVE ADS/DISCLAIMERS)
    # -----------------------------
    @staticmethod
    def clean_content(text):
        if not text:
            return text

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)

        patterns = [
            r"This page may contain affiliate links to legal sports betting partners\..*?FOX Sports may be compensated\..*?Sports Betting on FOX Sports\.?",
            r"affiliate links to legal sports betting partners.*?FOX Sports may be compensated.*?Sports Betting on FOX Sports\.?",
        ]

        for pattern in patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE)

        return text.strip()

    # -----------------------------
    # AUTHOR FROM HTML
    # -----------------------------
    @staticmethod
    def extract_author_from_html(soup):
        try:
            container = soup.select_one("div.article-contributors")
            if not container:
                return None

            name = container.select_one(".contributor-name")
            title = container.select_one(".contributor-title")

            if not name:
                return None

            return {
                "name": name.get_text(strip=True),
                "title": title.get_text(strip=True) if title else None,
            }

        except Exception:
            return None

    # -----------------------------
    # ARTICLE FETCH (ROBUST + RETRY)
    # -----------------------------
    @staticmethod
    def fetch_article(scraper, url):
        try:
            for attempt in range(3):
                response = scraper.get(
                    url,
                    headers=FoxSportsRSSPipeline.get_headers(),
                    timeout=30,
                )

                if response.status_code == 200:
                    break

                if response.status_code == 406:
                    logger.warning(f"406 blocked (retry {attempt+1}): {url}")
                    time.sleep(2 * (attempt + 1))
                    continue

            else:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            # AUTHOR
            html_author = FoxSportsRSSPipeline.extract_author_from_html(soup)
            authors_str = html_author["name"] if html_author else None

            scripts = soup.find_all("script", {"type": "application/ld+json"})

            for script in scripts:
                try:
                    if not script.string:
                        continue

                    data = json.loads(script.string)
                    graph = data.get("@graph", [])

                    for node in graph:
                        if node.get("@type") == "NewsArticle":

                            title = node.get("headline", "")

                            content = FoxSportsRSSPipeline.clean_content(
                                node.get("articleBody", "")
                            )

                            description = FoxSportsRSSPipeline.clean_content(
                                node.get("description", "")
                            )

                            if not authors_str:
                                authors = node.get("author", [])
                                if isinstance(authors, list):
                                    authors_str = ", ".join(
                                        [
                                            a.get("@id", "")
                                            for a in authors
                                            if isinstance(a, dict)
                                        ]
                                    )
                                else:
                                    authors_str = "FOX Sports Staff"

                            return {
                                "title": title,
                                "content": content or description,
                                "authors": authors_str,
                            }

                except Exception:
                    continue

            return None

        except Exception as e:
            logger.warning(f"Article fetch failed: {url} | {e}")
            return None

    # -----------------------------
    # FEED PARSER
    # -----------------------------
    @staticmethod
    def fetch_feed(feed_url):
        try:
            logger.info(f"Fetching feed: {feed_url}")

            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )

            response = scraper.get(
                feed_url,
                headers=FoxSportsRSSPipeline.get_headers(),
                timeout=30,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "xml")

            urls = soup.find_all("url")
            articles = []

            feed_build_date = datetime.now(timezone.utc)

            for url_node in urls:
                try:
                    loc = url_node.find("loc")
                    if not loc:
                        continue

                    link = loc.text.strip()

                    if link == "https://www.foxsports.com/":
                        continue

                    lastmod = url_node.find("lastmod")
                    pub_date = FoxSportsRSSPipeline.parse_date(
                        lastmod.text if lastmod else ""
                    )

                    if not FoxSportsRSSPipeline.is_within_7_days(pub_date):
                        continue

                    image_node = url_node.find("image:loc")
                    image = image_node.text if image_node else None

                    full_article = FoxSportsRSSPipeline.fetch_article(scraper, link)

                    if not full_article:
                        continue

                    title = full_article.get("title", "")
                    if "promo" in title.lower() or "bonus" in title.lower():
                        continue

                    content = full_article.get("content", "")
                    authors_str = full_article.get("authors", "FOX Sports Staff")

                    if len(content) < 200:
                        continue

                    articles.append(
                        {
                            "id": link,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "authors": authors_str,
                            "language": "en-US",
                            "image": image,
                            "source": FoxSportsRSSPipeline.SOURCE,
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

            logger.info(f"Parsed {len(articles)} FOX articles (7-day window)")
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

            for feed in FoxSportsRSSPipeline.FEEDS:
                all_articles.extend(FoxSportsRSSPipeline.fetch_feed(feed))

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles(all_articles, table_name=target_table, category="sports")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
