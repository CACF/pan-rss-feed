from config import FASHION_TABLE
import re
import uuid
import logging
import html
from datetime import datetime, timezone

import requests
import cloudscraper
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ArabNewsFashionRSSPipeline:

    SOURCE = "ArabNewsPK"

    RSS_FEEDS = [
        "https://www.arabnews.pk/taxonomy/term/14616/feed",
    ]

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

        return datetime.now(timezone.utc)

    # -----------------------------
    # CLEAN TEXT
    # -----------------------------
    @staticmethod
    def clean_text(raw_html: str) -> str:
        if not raw_html:
            return ""

        try:
            soup = BeautifulSoup(raw_html, "html.parser")

            # remove junk blocks
            for tag in soup.select(
                ".field-name-field-rbitem-author,"
                ".field-name-field-publication-date,"
                ".field-name-taxonomy-vocabulary-1,"
                ".field-name-taxonomy-vocabulary-9,"
                ".field-name-field-binary,"
                ".label"
            ):
                tag.decompose()

            # remove scripts / media / ads
            for tag in soup(["script", "style", "iframe", "noscript", "img", "video"]):
                tag.decompose()

            text = soup.get_text(" ", strip=True)

            # cleanup
            text = re.sub(r"http\S+|www\.\S+", "", text)
            text = re.sub(r"\s+", " ", text).strip()

            return text

        except Exception as e:
            logger.warning(f"clean_text failed: {e}")
            return ""

    # -----------------------------
    # CHECK CATEGORY
    # -----------------------------
    @staticmethod
    def is_fashion_related(title, categories):
        t = title.lower()
        keywords = [
            "fashion",
            "style",
            "outfit",
            "designer",
            "runway",
            "collection",
            "couture",
            "eid",
        ]

        if any(k in t for k in keywords):
            return True

        return any(c.lower() in ["fashion", "lifestyle"] for c in categories)

    # -----------------------------
    # FETCH FULL ARTICLE HTML
    # -----------------------------
    @staticmethod
    def fetch_article_content(url: str) -> str:
        try:
            scraper = cloudscraper.create_scraper()
            res = scraper.get(url, headers=get_random_headers(), timeout=30)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "html.parser")

            body = soup.select_one(".field-name-body")

            if body:
                return body.decode_contents()

            # fallback
            entry = soup.select_one(".entry-content")
            if entry:
                return entry.decode_contents()

            return res.text

        except Exception as e:
            logger.warning(f"Article fetch failed {url}: {e}")
            return ""

    # -----------------------------
    # HTML PAGE SCRAPER (FALLBACK)
    # -----------------------------
    @staticmethod
    def scrape_lifestyle_page():
        articles = []
        try:
            logger.info("Scraping ArabNews PK lifestyle page...")
            scraper = cloudscraper.create_scraper()
            res = scraper.get("https://www.arabnews.pk/lifestyle", headers=get_random_headers(), timeout=30)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "html.parser")
            raw_links = [
                a["href"] for a in soup.find_all("a", href=True)
                if ("/lifestyle/" in a["href"] or "/fashion/" in a["href"]) and a["href"].split("-")[-1].isdigit()
            ]
            unique_links = list(set(raw_links))
            feed_time = datetime.now(timezone.utc)

            for rel_link in unique_links:
                full_url = "https://www.arabnews.pk" + rel_link if rel_link.startswith("/") else rel_link
                try:
                    content_html = ArabNewsFashionRSSPipeline.fetch_article_content(full_url)
                    content = ArabNewsFashionRSSPipeline.clean_text(content_html)
                    if len(content) < 150:
                        continue

                    # Extract title from link or page
                    slug = rel_link.split("/")[-1].rsplit("-", 1)[0]
                    title = slug.replace("-", " ").title()

                    articles.append(
                        {
                            "id": full_url,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": feed_time,
                            "feedBuildDate": feed_time,
                            "title": title,
                            "authors": "Arab News",
                            "language": "en-US",
                            "image": None,
                            "source": ArabNewsFashionRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Fashion",
                            "media_origin": "local",
                            "tags": ["fashion", "lifestyle"],
                        }
                    )
                except Exception as e:
                    logger.debug(f"Failed scraping article {full_url}: {e}")

            logger.info(f"Scraped {len(articles)} articles from ArabNews lifestyle page")
        except Exception as e:
            logger.info(f"ArabNews lifestyle page fallback error: {e}")
        return articles

    # -----------------------------
    # RSS FETCH
    # -----------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching RSS: {feed_url}")

            scraper = cloudscraper.create_scraper()
            response = scraper.get(feed_url, headers=get_random_headers(), timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "lxml-xml")
            items = soup.find_all("item")

            articles = []
            feed_time = datetime.now(timezone.utc)

            for item in items:
                try:
                    title = item.find("title")
                    link = item.find("link")

                    if not title or not link:
                        continue

                    title_text = title.get_text(strip=True)
                    link_text = link.get_text(strip=True)

                    categories = [
                        c.get_text(strip=True) for c in item.find_all("category")
                    ]

                    if not ArabNewsFashionRSSPipeline.is_fashion_related(
                        title_text, categories
                    ):
                        continue

                    pub_date_elem = item.find("pubDate")
                    pub_date = ArabNewsFashionRSSPipeline.parse_date(
                        pub_date_elem.get_text() if pub_date_elem else None
                    )

                    content_html = ArabNewsFashionRSSPipeline.fetch_article_content(
                        link_text
                    )
                    content = ArabNewsFashionRSSPipeline.clean_text(content_html)

                    if len(content) < 150:
                        continue

                    creator = item.find("dc:creator")
                    author = creator.get_text(strip=True) if creator else "Arab News"

                    articles.append(
                        {
                            "id": link_text,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_time,
                            "title": title_text,
                            "authors": author,
                            "language": "en-US",
                            "image": None,
                            "source": ArabNewsFashionRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Fashion",
                            "media_origin": "local",
                            "tags": categories or ["fashion"],
                        }
                    )

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")

            logger.info(f"Parsed {len(articles)} articles")
            return articles

        except Exception as e:
            logger.info(f"RSS fetch failed ({e}). Falling back to HTML scraping...")
            return ArabNewsFashionRSSPipeline.scrape_lifestyle_page()

    # -----------------------------
    # RUN
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = FASHION_TABLE
            all_articles = []

            for feed in ArabNewsFashionRSSPipeline.RSS_FEEDS:
                all_articles.extend(ArabNewsFashionRSSPipeline.fetch_rss_feed(feed))

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles_current_year(all_articles, table_name=target_table)

        except Exception as e:
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
