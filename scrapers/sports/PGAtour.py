from config import SPORTS_TABLE
import uuid
import time
import logging
import re
import concurrent.futures
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class PGATourRSSPipeline:
    SOURCE = "PGA Tour"
    RSS_FEEDS = [
        "https://www.pgatour.com/sitemap/articles.xml",
    ]

    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(html):
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return re.sub(r"http\S+|www\.\S+", "", text)

    @staticmethod
    def fetch_full_description(link):
        """
        Fetch full PGA Tour article content.
        Uses stable selectors (title, author, content). No length filters.
        """
        author = "Unknown"
        content = None

        try:
            with cloudscraper.create_scraper() as scraper:
                res = scraper.get(
                    link,
                    timeout=15,
                    headers=get_random_headers(PGATourRSSPipeline.headers),
                )

                if res.status_code != 200:
                    return None, author

                soup = BeautifulSoup(res.content, "lxml")

                # Author: <span class="chakra-text css-yb1mvg">Written by Rob Bolton</span>
                author_elem = soup.select_one("span.css-yb1mvg")
                if author_elem:
                    raw_author = author_elem.get_text(strip=True)
                    author = (
                        re.sub(
                            r"^written by\s*", "", raw_author, flags=re.IGNORECASE
                        ).strip()
                        or "Unknown"
                    )

                # Content: paragraph blocks inside the article body wrapper
                paragraphs = soup.select("div.css-1kvgtbr > p")

                text_parts = [
                    re.sub(r"http\S+|www\.\S+", "", p.get_text(strip=True))
                    for p in paragraphs
                    if p.get_text(strip=True)
                ]

                if text_parts:
                    content = " ".join(text_parts)

                res.close()

        except Exception as e:
            logger.warning(f"PGA Tour article fetch failed: {link} | {e}")

        return content, author

    @staticmethod
    def fetch_pga_rss_feed(feed_url):
        try:
            logger.info(f"Fetching PGA Tour sitemap feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=15,
                    headers=get_random_headers(PGATourRSSPipeline.headers),
                )
                response.raise_for_status()
                payload = response.content
                response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("url")

            feed_date = datetime.now(timezone.utc)

            base_articles = []
            for item in items:
                loc_elem = item.find("loc")
                if not loc_elem:
                    continue
                link = loc_elem.get_text(strip=True)

                news_elem = item.find("news")
                title = ""
                pub_date = feed_date

                if news_elem:
                    title_elem = news_elem.find("title")
                    if title_elem:
                        title = title_elem.get_text(strip=True)

                    date_elem = news_elem.find("publication_date")
                    if date_elem:
                        pub_date = PGATourRSSPipeline.parse_date(
                            date_elem.get_text(strip=True)
                        )

                base_articles.append(
                    {
                        "title": title,
                        "link": link,
                        "description": "",
                        "pub_date": pub_date,
                        "feed_date": feed_date,
                    }
                )

            articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_map = {
                    executor.submit(
                        PGATourRSSPipeline.fetch_full_description, a["link"]
                    ): a
                    for a in base_articles
                }

                for future in concurrent.futures.as_completed(future_map):
                    base = future_map[future]
                    try:
                        full_content, author = future.result()
                        content = full_content or base["description"]

                        if not content:
                            continue

                        article = {
                            "id": base["link"],
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": base["pub_date"],
                            "feedBuildDate": base["feed_date"],
                            "title": base["title"],
                            "authors": author,
                            "language": "en",
                            "source": PGATourRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Golf",
                            "media_origin": "foreign",
                            "tags": [],
                        }

                        articles.append(article)

                    except Exception as e:
                        logger.warning(f"PGA Tour article processing error: {e}")

            logger.info(f"Parsed {len(articles)} valid PGA Tour articles")
            return articles

        except Exception as e:
            logger.error(f"PGA Tour RSS fetch failed: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        all_articles = []
        for feed in PGATourRSSPipeline.RSS_FEEDS:
            all_articles.extend(PGATourRSSPipeline.fetch_pga_rss_feed(feed))
        logger.info(f"PGA Tour RSS pipeline processed {len(all_articles)} articles")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = SPORTS_TABLE
            all_articles = PGATourRSSPipeline.process_input()

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="sports"
            )

            return result

        except Exception as e:
            logger.error(f"PGA Tour RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
