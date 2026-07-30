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


class WorldSoccerRSSPipeline:
    SOURCE = "World Soccer"
    RSS_FEEDS = [
        "https://www.worldsoccer.com/feed",
    ]

    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%a, %d %b %Y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
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
        Fetch full World Soccer article content.
        World Soccer runs on WordPress, so we target the standard
        WordPress entry-content wrapper and dc:creator-style byline.
        """
        author = "Unknown"
        content = None

        try:
            with cloudscraper.create_scraper() as scraper:
                res = scraper.get(
                    link,
                    timeout=15,
                    headers=get_random_headers(WorldSoccerRSSPipeline.headers),
                )

                if res.status_code != 200:
                    return None, author

                soup = BeautifulSoup(res.content, "lxml")

                author_elem = soup.select_one(
                    "a[rel='author'], span.author, "
                    "div.author-name a, meta[name='author']"
                )
                if author_elem:
                    author = (
                        author_elem.get("content")
                        if author_elem.name == "meta"
                        else author_elem.get_text(strip=True)
                    )

                paragraphs = soup.select("div.editable-content > p")

                text_parts = [
                    re.sub(r"http\S+|www\.\S+", "", p.get_text(strip=True))
                    for p in paragraphs
                    if p.get_text(strip=True)
                ]

                if text_parts:
                    content = " ".join(text_parts)

                res.close()

        except Exception as e:
            logger.warning(f"World Soccer article fetch failed: {link} | {e}")

        return content, author

    @staticmethod
    def fetch_world_soccer_rss_feed(feed_url):
        try:
            logger.info(f"Fetching World Soccer RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=15,
                    headers=get_random_headers(WorldSoccerRSSPipeline.headers),
                )
                response.raise_for_status()
                payload = response.content
                response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            feed_date_elem = soup.find("lastBuildDate")
            feed_date = (
                WorldSoccerRSSPipeline.parse_date(feed_date_elem.text)
                if feed_date_elem
                else datetime.now(timezone.utc)
            )

            base_articles = []
            for item in items:
                title = item.title.get_text(strip=True)
                link = item.link.get_text(strip=True)
                pub_date = (
                    WorldSoccerRSSPipeline.parse_date(item.pubDate.text)
                    if item.pubDate
                    else datetime.now(timezone.utc)
                )
                desc = (
                    WorldSoccerRSSPipeline.clean_content(item.description.text)
                    if item.description
                    else ""
                )

                # dc:creator holds the author in this feed (e.g. "Henry Winter")
                creator_elem = item.find("dc:creator") or item.find("creator")
                creator = creator_elem.get_text(strip=True) if creator_elem else None

                # categories can be used as tags (e.g. "2026 World Cup", "Latest")
                categories = [c.get_text(strip=True) for c in item.find_all("category")]

                base_articles.append(
                    {
                        "title": title,
                        "link": link,
                        "description": desc,
                        "pub_date": pub_date,
                        "feed_date": feed_date,
                        "creator": creator,
                        "categories": categories,
                    }
                )

            articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_map = {
                    executor.submit(
                        WorldSoccerRSSPipeline.fetch_full_description, a["link"]
                    ): a
                    for a in base_articles
                }

                for future in concurrent.futures.as_completed(future_map):
                    base = future_map[future]
                    try:
                        full_content, scraped_author = future.result()
                        content = full_content or base["description"]

                        if not content:
                            continue

                        # Prefer the feed's dc:creator (more reliable than scraping)
                        author = base["creator"] or scraped_author

                        article = {
                            "id": base["link"],
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": base["pub_date"],
                            "feedBuildDate": base["feed_date"],
                            "title": base["title"],
                            "authors": author,
                            "language": "en",
                            "source": WorldSoccerRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Football",
                            "media_origin": "foreign",
                            "tags": base["categories"],
                        }

                        articles.append(article)

                    except Exception as e:
                        logger.warning(f"World Soccer article processing error: {e}")

            logger.info(f"Parsed {len(articles)} valid World Soccer articles")
            return articles

        except Exception as e:
            logger.error(f"World Soccer RSS fetch failed: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        all_articles = []
        for feed in WorldSoccerRSSPipeline.RSS_FEEDS:
            all_articles.extend(
                WorldSoccerRSSPipeline.fetch_world_soccer_rss_feed(feed)
            )
        logger.info(f"World Soccer RSS pipeline processed {len(all_articles)} articles")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = SPORTS_TABLE
            all_articles = WorldSoccerRSSPipeline.process_input()

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(f"After dedupe: {len(all_articles)} articles")

            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table, category="sports"
            )

            return result

        except Exception as e:
            logger.error(f"World Soccer RSS pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
