from config import FASHION_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class HMagFashionPipeline:
    """
    HELLO! Pakistan Fashion Scraper (HTML-based)
    """

    SOURCE = "HMagPakistan"
    BASE_URL = "https://www.hmagpak.com/fashion"

    # -----------------------------
    # CLEAN TEXT
    # -----------------------------
    @staticmethod
    def clean_text(html):
        if not html:
            return ""

        try:
            soup = BeautifulSoup(html, "html.parser")

            # remove junk aggressively
            for tag in soup(
                [
                    "script",
                    "style",
                    "iframe",
                    "noscript",
                    "img",
                    "figure",
                    "button",
                    "form",
                    "svg",
                ]
            ):
                tag.decompose()

            # remove common UI noise blocks
            for tag in soup.select(".entry-meta, .share, .tags, .related-posts"):
                tag.decompose()

            text = soup.get_text(separator=" ")

            # remove URLs
            text = re.sub(r"http\S+|www\.\S+", "", text)

            # normalize whitespace
            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"clean_text failed: {e}")
            return html or ""

    # -----------------------------
    # PARSE LISTING PAGE
    # -----------------------------
    @staticmethod
    def parse_listing(html):
        soup = BeautifulSoup(html, "html.parser")

        items = soup.select("div.single-featured")

        articles = []

        for item in items:
            try:
                a_tag = item.select_one(".tt-news-img a")
                img_tag = item.select_one("img")

                if not a_tag:
                    continue

                link = a_tag.get("href")
                title_tag = item.select_one("h3")

                title = title_tag.get_text(strip=True) if title_tag else ""

                image = img_tag.get("src") if img_tag else None

                articles.append({"title": title, "link": link, "image": image})

            except Exception as e:
                logger.warning(f"Listing parse error: {e}")
                continue

        return articles

    # -----------------------------
    # PARSE DETAIL PAGE
    # -----------------------------
    @staticmethod
    def parse_detail(html):
        soup = BeautifulSoup(html, "html.parser")

        try:
            # title
            title_tag = soup.select_one(".article-content h3, h1")
            title = title_tag.get_text(strip=True) if title_tag else ""

            # author
            author_tag = soup.select_one('.entry-meta a[href*="author"]')
            author = author_tag.get_text(strip=True) if author_tag else "HMag Pakistan"

            # image
            img_tag = soup.select_one(".article-image img, .article-content img")
            image = img_tag.get("src") if img_tag else None

            # IMPORTANT: isolate real article body only
            content_div = soup.select_one("div.blog-details-desc")

            if content_div:
                # remove junk inside content block before conversion
                for tag in content_div.select(".entry-meta, script, style, iframe"):
                    tag.decompose()

            raw_html = str(content_div) if content_div else ""
            content = HMagFashionPipeline.clean_text(raw_html)

            # optional date
            date_tag = soup.select_one(".entry-meta li a")
            date_text = date_tag.get_text(strip=True) if date_tag else ""

            pub_date = datetime.now(timezone.utc)

            return {
                "title": title,
                "content": content,
                "image": image,
                "author": author,
                "pub_date": pub_date,
                "raw_date": date_text,
            }

        except Exception as e:
            logger.warning(f"Detail parse failed: {e}")
            return None

    # -----------------------------
    # FETCH LISTING
    # -----------------------------
    @staticmethod
    def fetch_listing():
        try:
            logger.info("Fetching HMag fashion listing")

            response = requests.get(
                HMagFashionPipeline.BASE_URL,
                headers=get_random_headers(),
                timeout=30,
            )
            response.raise_for_status()

            return HMagFashionPipeline.parse_listing(response.text)

        except Exception as e:
            logger.error(f"Listing fetch failed: {e}")
            return []

    # -----------------------------
    # FETCH DETAIL
    # -----------------------------
    @staticmethod
    def fetch_detail(url):
        try:
            response = requests.get(
                url,
                headers=get_random_headers(),
                timeout=30,
            )
            response.raise_for_status()

            return HMagFashionPipeline.parse_detail(response.text)

        except Exception as e:
            logger.warning(f"Detail fetch failed {url}: {e}")
            return None

    # -----------------------------
    # PIPELINE RUN
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = FASHION_TABLE
            feed_items = HMagFashionPipeline.fetch_listing()

            if not feed_items:
                return {"inserted_count": 0, "total_articles": 0}

            articles = []

            for item in feed_items:
                detail = HMagFashionPipeline.fetch_detail(item["link"])
                if not detail:
                    continue

                content = detail["content"]

                # skip junk articles
                if not content or len(content) < 250:
                    continue

                articles.append(
                    {
                        "id": item["link"],
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": detail["pub_date"],
                        "feedBuildDate": datetime.now(timezone.utc),
                        "title": detail["title"] or item["title"],
                        "authors": detail.get("author") or "HMag Pakistan",
                        "language": "en-US",
                        "image": detail.get("image") or item.get("image"),
                        "source": HMagFashionPipeline.SOURCE,
                        "content": content,
                        "genre": "Fashion",
                        "media_origin": "local",
                        "tags": [],
                    }
                )

            logger.info(f"Parsed {len(articles)} HMag articles")

            if not articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles_current_year(articles, table_name=target_table)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
