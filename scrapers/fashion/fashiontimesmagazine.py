from config import FASHION_TABLE
import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class FashionTimesRSSPipeline:
    """
    Fashion Times Magazine RSS Pipeline
    """

    SOURCE = "FashionTimes"

    RSS_FEEDS = [
        "https://fashiontimesmagazine.com/feed/",
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

            # remove junk elements
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
    # Fashion Times' own site nav groups its content into these categories
    # (see the "Blogs" menu on fashiontimesmagazine.com). The RSS feed's
    # latest items skew heavily toward Entertainment/Interviews (celebrity,
    # drama, red-carpet coverage) rather than the narrower "Fashion" tag,
    # so restricting to just fashion/lifestyle/events left almost nothing
    # through. Sports/Food/Politics are still excluded as genuinely
    # off-topic for a fashion table.
    FASHION_CATEGORIES = {
        "fashion",
        "lifestyle",
        "events",
        "entertainment",
        "interviews",
        "magazine issues",
        "15 questions with fashion times",
    }

    FASHION_KEYWORDS = [
        "fashion",
        "style",
        "styles",
        "outfit",
        "designer",
        "lawn",
        "celebrity",
        "eid",
        "runway",
        "collection",
        "couture",
        "glamour",
        "red carpet",
        "wardrobe",
        "look",
        "looks",
        "cannes",
        "gala",
        "bridal",
    ]

    @staticmethod
    def is_fashion_related(title, categories):
        title_lower = title.lower()

        if any(k in title_lower for k in FashionTimesRSSPipeline.FASHION_KEYWORDS):
            return True

        for cat in categories:
            if cat.strip().lower() in FashionTimesRSSPipeline.FASHION_CATEGORIES:
                return True

        return False

    # -----------------------------
    # FETCH RSS
    # -----------------------------
    @staticmethod
    def fetch_rss_feed(feed_url):
        try:
            logger.info(f"Fetching Fashion Times RSS: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=30,
                    headers=get_random_headers(),
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    content_elem = item.find("content:encoded")
                    desc_elem = item.find("description")
                    category_elems = item.find_all("category")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)

                    categories = [c.get_text(strip=True) for c in category_elems if c]

                    pub_date = (
                        FashionTimesRSSPipeline.parse_date(pub_date_elem.get_text())
                        if pub_date_elem
                        else datetime.now(timezone.utc)
                    )

                    raw_content = (
                        content_elem.get_text()
                        if content_elem
                        else desc_elem.get_text() if desc_elem else ""
                    )

                    content = FashionTimesRSSPipeline.clean_text(raw_content)

                    # skip low quality posts
                    if len(content) < 50:
                        logger.info(f"Skipped (too short): {title}")
                        continue

                    # NOTE: topic filtering is disabled by default. This feed's
                    # <category> tags are just "Miscellaneous" plus free-form
                    # hashtags (not the site's real Fashion/Entertainment/etc.
                    # taxonomy), so is_fashion_related() has no reliable signal
                    # to work with, and this source is treated the same way as
                    # every other pipeline: whatever it publishes goes in. To
                    # re-enable topic filtering, uncomment below.
                    #
                    # if not FashionTimesRSSPipeline.is_fashion_related(title, categories):
                    #     logger.info(f"Skipped (not fashion): {title} | categories={categories}")
                    #     continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": "Fashion Times",
                        "language": "en-US",
                        "image": None,
                        "source": FashionTimesRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Fashion",
                        "media_origin": "local",
                        "tags": categories,
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} Fashion Times articles.")
            return articles

        except Exception as e:
            logger.error(f"Fashion Times RSS fetch failed: {e}")
            return []

    # -----------------------------
    # PIPELINE RUN
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = table_name or FASHION_TABLE
            all_articles = []

            for feed_url in FashionTimesRSSPipeline.RSS_FEEDS:
                articles = FashionTimesRSSPipeline.fetch_rss_feed(feed_url)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            # Supabase insert (current year logic)
            result = SupabaseClient.insert_articles_current_year(all_articles, table_name=target_table)

            return result

        except Exception as e:
            logger.error(f"Fashion Times pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }