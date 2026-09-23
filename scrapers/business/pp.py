from config import BUSINESS_TABLE
import uuid
import logging
import time
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import requests
import cloudscraper
from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ProfitPakistanTodayRSSPipeline:
    """
    Profit by Pakistan Today RSS feed pipeline that fetches, parses,
    and stores Profit news articles.
    """

    SOURCE = "Profit by Pakistan Today"
    RSS_FEEDS = [
        "https://profit.pakistantoday.com.pk/feed/",
    ]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        """Parse RSS pubDate format to datetime."""
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
        """Convert HTML to clean plain text (no links, no scripts)."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style", "aside", "figure", "iframe"]):
                tag.decompose()

            for a in soup.find_all("a"):
                a.unwrap()

            text = soup.get_text(separator=" ", strip=True)
            text = " ".join(
                word for word in text.split() if not word.startswith("http")
            )

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    @staticmethod
    def scrape_html_page():
        articles = []
        try:
            logger.info("Scraping Profit by Pakistan Today home page...")
            scraper = cloudscraper.create_scraper()
            res = scraper.get("https://profit.pakistantoday.com.pk/", timeout=30)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "html.parser")
            raw_links = [
                a["href"] for a in soup.find_all("a", href=True)
                if "/20" in a["href"]
            ]
            unique_links = list(set([
                "https://profit.pakistantoday.com.pk" + l if l.startswith("/") else l
                for l in raw_links
            ]))
            feed_time = datetime.now(timezone.utc)

            for url in unique_links[:20]:
                try:
                    art_res = scraper.get(url, timeout=20)
                    if art_res.status_code != 200:
                        continue
                    art_soup = BeautifulSoup(art_res.text, "html.parser")

                    title_elem = art_soup.find("h1")
                    title = title_elem.get_text(strip=True) if title_elem else ""
                    if not title:
                        continue

                    paragraphs = [
                        p.get_text(strip=True) for p in art_soup.find_all("p")
                        if len(p.get_text(strip=True)) > 30 and not any(x in p.get_text().lower() for x in ["copyright", "subscribe", "all rights reserved"])
                    ]
                    content = ProfitPakistanTodayRSSPipeline.clean_content(" ".join(paragraphs))
                    if len(content) < 150:
                        continue

                    author_elem = art_soup.select_one(".td-post-author-name a, .author a, meta[name='author']")
                    author = author_elem.get_text(strip=True) if author_elem else "Profit Staff"

                    articles.append({
                        "id": url,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": feed_time,
                        "feedBuildDate": feed_time,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": ProfitPakistanTodayRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    })
                except Exception as e:
                    logger.debug(f"Failed scraping article {url}: {e}")

            logger.info(f"Scraped {len(articles)} articles from Profit home page")
        except Exception as e:
            logger.info(f"Profit home page fallback error: {e}")
        return articles

    @staticmethod
    def fetch_profit_rss_feed(feed_url):
        """Fetch, parse, and return Profit RSS feed articles."""
        try:
            logger.info(f"Fetching Profit RSS feed: {feed_url}")
            response = requests.get(
                feed_url,
                timeout=30,
                headers=ProfitPakistanTodayRSSPipeline.headers,
            )
            response.raise_for_status()

            payload = response.content
            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title = item.find("title").get_text(strip=True)
                    link = item.find("link").get_text(strip=True)
                    pub_date = item.find("pubDate")

                    article_pub_date = (
                        ProfitPakistanTodayRSSPipeline.parse_date(pub_date.get_text())
                        if pub_date
                        else feed_build_date
                    )

                    creator = item.find("dc:creator")
                    author = creator.get_text(strip=True) if creator else "Profit Staff"

                    encoded = item.find("content:encoded")
                    description = item.find("description")

                    content_raw = (
                        encoded.get_text()
                        if encoded
                        else (description.get_text() if description else "")
                    )

                    content = ProfitPakistanTodayRSSPipeline.clean_content(content_raw)

                    if len(content) < 150:
                        continue

                    article = {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": title,
                        "authors": author,
                        "language": "en-us",
                        "source": ProfitPakistanTodayRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Business",
                        "media_origin": "local",
                        "tags": [],
                    }

                    articles.append(article)

                except Exception as e:
                    logger.warning(f"Failed to process Profit article: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.info(f"Profit RSS fetch failed ({e}). Falling back to HTML scraping...")
            return ProfitPakistanTodayRSSPipeline.scrape_html_page()

    @staticmethod
    def process_input(input_data=None):
        """Process all RSS feeds concurrently."""
        try:
            logger.info("Starting Profit RSS pipeline (concurrent)")
            all_articles = []
            max_workers = 5

            def _fetch(feed):
                return ProfitPakistanTodayRSSPipeline.fetch_profit_rss_feed(feed)

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_feed = {
                    executor.submit(_fetch, feed): feed
                    for feed in ProfitPakistanTodayRSSPipeline.RSS_FEEDS
                }

                for future in concurrent.futures.as_completed(future_to_feed):
                    feed = future_to_feed[future]
                    try:
                        articles = future.result()
                        all_articles.extend(articles)
                        logger.info(f"Feed processed: {feed} -> {len(articles)} articles")
                    except Exception:
                        logger.exception(f"Feed failed: {feed}")
                        continue

            logger.info(f"Profit pipeline processed {len(all_articles)} total articles")
            return all_articles

        except Exception as e:
            logger.error(f"Profit RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = BUSINESS_TABLE
            all_articles = []

            for feed_url in ProfitPakistanTodayRSSPipeline.RSS_FEEDS:
                articles = ProfitPakistanTodayRSSPipeline.fetch_profit_rss_feed(feed_url)
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
            logger.error(f"Profit RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
