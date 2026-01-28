import uuid
import logging
import time
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper
from app.utilities import MongoDBClient, get_random_headers

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
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
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
            text = " ".join(word for word in text.split() if not word.startswith("http"))

            return " ".join(text.split())

        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    @staticmethod
    def fetch_profit_rss_feed(feed_url, max_retries=3):
        """Fetch and parse a single Profit RSS feed using cloudscraper."""
        scraper = cloudscraper.create_scraper()
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Fetching Profit RSS feed (attempt {attempt}): {feed_url}")
                headers = get_random_headers(ProfitPakistanTodayRSSPipeline.headers)
                response = scraper.get(feed_url, timeout=30, headers=headers)
                breakpoint()
                response.raise_for_status()
                payload = response.content

                soup = BeautifulSoup(payload, "lxml-xml")
                items = soup.find_all("item")
                feed_build_date = datetime.now(timezone.utc)

                if not items:
                    logger.warning(f"No items found in feed: {feed_url}")
                    return []

                articles = []
                for item in items:
                    
                    try:
                        title_elem = item.find("title")
                        link_elem = item.find("link")
                        guid_elem = item.find("guid")
                        desc_elem = item.find("description")
                        pub_date_elem = item.find("pubDate")
                        category_elem = item.find("category")
                        author_elem = item.find("dc:creator")
                        content_encoded_elem = item.find("content:encoded")

                        if not title_elem or not link_elem:
                            continue

                        title = title_elem.get_text(strip=True)
                        link = link_elem.get_text(strip=True)
                        guid = guid_elem.get_text(strip=True) if guid_elem else link
                        pub_date = pub_date_elem.get_text(strip=True) if pub_date_elem else ""
                        category = category_elem.get_text(strip=True) if category_elem else "Business"
                        author = author_elem.get_text(strip=True) if author_elem else "Profit Desk"

                        content_html = (
                            content_encoded_elem.get_text() if content_encoded_elem else
                            (desc_elem.get_text() if desc_elem else "")
                        )
                        content = ProfitPakistanTodayRSSPipeline.clean_content(content_html)

                        article = {
                            "_id": guid,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": ProfitPakistanTodayRSSPipeline.parse_date(pub_date),
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "authors": author,
                            "language": "en-us",
                            "source": ProfitPakistanTodayRSSPipeline.SOURCE,
                            "content": content,
                            "genre": category,
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
                logger.warning(f"Attempt {attempt} failed for {feed_url}: {e}")
                if attempt == max_retries:
                    logger.error(f"Failed to fetch Profit RSS feed {feed_url} after {max_retries} attempts")
                    return []
                time.sleep(2 ** attempt)  # exponential backoff

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
    def run_pipeline(input_data=None):
        """Run the complete Profit RSS pipeline."""
        try:
            logger.info("Starting Profit RSS pipeline")
            t0 = time.perf_counter()

            articles = ProfitPakistanTodayRSSPipeline.process_input(input_data)

            if not articles:
                elapsed = round(time.perf_counter() - t0, 2)
                logger.warning("No Profit articles to insert")
                return {"inserted_count": 0, "total_articles": 0, "elapsed_time": elapsed}

            result = MongoDBClient.insert_articles_to_mongo(
                articles,
                user_email=input_data.get("email") if input_data else None,
            )
            result["elapsed_time"] = round(time.perf_counter() - t0, 2)
            logger.info(f"Profit pipeline finished in {result['elapsed_time']}s")
            return result

        except Exception as e:
            elapsed = round(time.perf_counter() - t0, 2)
            logger.error(f"Profit RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
                "elapsed_time": elapsed,
            }
