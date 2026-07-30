from config import SPORTS_TABLE
import re
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import concurrent.futures
import requests
from urllib.parse import urljoin
import time

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class SportsnetScraper:
    SOURCE = "Sportsnet"
    SECTION_URLS = [
        "https://www.sportsnet.ca/soccer/fifa-world-cup/",
        "https://www.sportsnet.ca/football/cfl/",
        "https://www.sportsnet.ca/mma/ufc/",
        "https://www.sportsnet.ca/basketball/nba/",
        "https://www.sportsnet.ca/baseball/mlb/",
        "https://www.sportsnet.ca/hockey/nhl/",
    ]

    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # wp_post_type values we want to keep as "articles". bc-video is a video post, not an article.
    ARTICLE_POST_TYPES = {"sn-article"}

    # Only keep articles published within this many days
    MAX_ARTICLE_AGE_DAYS = 7

    @staticmethod
    def parse_date(date_str: str):
        """Parse Sportsnet date strings to datetime."""
        try:
            if not date_str:
                return datetime.now(timezone.utc)

            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

            try:
                import dateparser

                dt = dateparser.parse(date_str, settings={"TIMEZONE": "UTC"})
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
            except ImportError:
                pass

            formats = [
                "%B %d, %Y at %I:%M %p",
                "%B %d, %Y",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%b %d, %Y",
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue

            logger.warning(f"Could not parse date '{date_str}', using current time")
            return datetime.now(timezone.utc)

        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(html):
        """Clean article HTML content."""
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(
                ["script", "style", "aside", "iframe", "noscript", "button"]
            ):
                tag.decompose()
            for a in soup.find_all("a"):
                a.unwrap()
            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)
            text = re.sub(r"\s+", " ", text)
            return text.strip()
        except Exception as e:
            logger.warning(f"clean_content error: {e}")
            return html

    @classmethod
    def extract_genre_from_url(cls, url):
        """Extract genre from URL."""
        url_lower = url.lower()
        if "soccer" in url_lower or "fifa-world-cup" in url_lower:
            return "Soccer"
        elif "football" in url_lower or "cfl" in url_lower:
            return "Football"
        elif "mma" in url_lower or "ufc" in url_lower:
            return "MMA"
        elif "basketball" in url_lower or "nba" in url_lower:
            return "Basketball"
        elif "baseball" in url_lower or "mlb" in url_lower:
            return "Baseball"
        elif "hockey" in url_lower or "nhl" in url_lower:
            return "Hockey"
        else:
            return "Sports"

    @staticmethod
    def _extract_next_data(soup):
        """Pull and parse the __NEXT_DATA__ JSON blob that Sportsnet's Next.js
        pages embed. All article/collection data lives here; the visible DOM
        is just skeleton-loader placeholders until JS runs client-side."""
        script_tag = soup.select_one("script#__NEXT_DATA__")
        if not script_tag or not script_tag.string:
            return None
        try:
            return json.loads(script_tag.string)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse __NEXT_DATA__ JSON: {e}")
            return None

    @classmethod
    def _walk_collections(cls, node, out, wanted_types):
        """Recursively walk the page-data tree, collecting permalinks for any
        item whose wp_post_type is in wanted_types. Handles both items nested
        inside a collection's collection_data array and standalone top-level
        items (e.g. layout: 'single_small' / 'single_large')."""
        if isinstance(node, dict):
            post_type = node.get("wp_post_type")
            permalink = node.get("sn_custom_permalink")
            if post_type in wanted_types and permalink:
                out.append(permalink)

            collection_data = node.get("collection_data")
            if collection_data:
                cls._walk_collections(collection_data, out, wanted_types)

        elif isinstance(node, list):
            for item in node:
                cls._walk_collections(item, out, wanted_types)

    @classmethod
    def fetch_article_links(cls):
        """Get links to Sportsnet articles from all sections via the
        __NEXT_DATA__ JSON blob (the page is a Next.js SPA with no article
        content in the server-rendered DOM)."""
        try:
            all_links = []

            for section_url in cls.SECTION_URLS:
                logger.info(f"Fetching article links from: {section_url}")

                try:
                    resp = requests.get(
                        section_url, timeout=30, headers=get_random_headers(cls.HEADERS)
                    )
                    resp.raise_for_status()
                    payload = resp.content
                    soup = BeautifulSoup(payload, "lxml")

                    data = cls._extract_next_data(soup)
                    if not data:
                        logger.warning(
                            f"No __NEXT_DATA__ found on {section_url}, skipping"
                        )
                        continue

                    try:
                        top_level = data["props"]["pageProps"]["page"]["data"]
                    except (KeyError, TypeError):
                        logger.warning(
                            f"Unexpected __NEXT_DATA__ structure on {section_url}"
                        )
                        continue

                    section_links = []
                    cls._walk_collections(
                        top_level, section_links, cls.ARTICLE_POST_TYPES
                    )

                    logger.info(
                        f"Found {len(section_links)} article links on {section_url}"
                    )

                    for href in section_links:
                        if href.startswith("/"):
                            href = "https://www.sportsnet.ca" + href
                        elif not href.startswith("http"):
                            href = urljoin(section_url, href)

                        if href and "sportsnet.ca" in href:
                            if not any(
                                x in href
                                for x in [
                                    "/live/",
                                    "/video/",
                                    "/radio/",
                                    "/news/",
                                    "/tag/",
                                    "/search",
                                ]
                            ):
                                all_links.append(href)

                except Exception as e:
                    logger.error(f"Error fetching from {section_url}: {e}")
                    continue

                time.sleep(0.5)

            all_links = list(set(all_links))
            logger.info(f"Total unique article links found: {len(all_links)}")

            if all_links:
                logger.info(f"Sample links: {all_links[:3]}")

            return all_links

        except Exception as e:
            logger.error(f"Failed to fetch article links: {e}")
            return []

    @classmethod
    def fetch_article(cls, url):
        """Fetch and parse an individual Sportsnet article."""
        try:
            logger.debug(f"Fetching article: {url}")

            resp = requests.get(
                url, timeout=8, headers=get_random_headers(cls.HEADERS)
            )
            resp.raise_for_status()
            payload = resp.content
            soup = BeautifulSoup(payload, "html.parser")

            title_elem = soup.select_one("header h1")
            if not title_elem:
                title_elem = soup.select_one(
                    "h1.detailHeadline, h1.article-title, h1.title, h1"
                )

            title = title_elem.get_text(strip=True) if title_elem else ""

            if not title:
                logger.debug(f"No title found for {url}")
                return None

            time_elem = soup.select_one(".micro_authors-time-wrapper > time")
            pub_date = datetime.now(timezone.utc)
            if time_elem:
                date_str = time_elem.get("datetime") or time_elem.get_text(strip=True)
                if date_str:
                    pub_date = cls.parse_date(date_str)

            # Skip articles older than MAX_ARTICLE_AGE_DAYS
            cutoff = datetime.now(timezone.utc) - timedelta(
                days=cls.MAX_ARTICLE_AGE_DAYS
            )
            if pub_date < cutoff:
                logger.debug(
                    f"Skipping stale article ({pub_date.isoformat()}) for {url}"
                )
                return None

            author_elem = soup.select_one(".micro_author > span > a:first-of-type")
            if not author_elem:
                author_elem = soup.select_one(
                    ".bylineDetails .authorText a, .author a, .byline a"
                )
            author = (
                author_elem.get_text(strip=True) if author_elem else "Sportsnet Staff"
            )

            content_paragraphs = []

            article_body = soup.select_one(".components_article-body")
            if article_body:
                paragraphs = article_body.select(".components_paragraph p")
                for p in paragraphs:
                    text = p.get_text(" ", strip=True)
                    if (
                        text
                        and len(text) > 10
                        and not text.lower().startswith("copyright")
                    ):
                        content_paragraphs.append(text)

            if not content_paragraphs:
                story_div = soup.select_one("div.story")
                if story_div:
                    paragraphs = story_div.select("p")
                    for p in paragraphs:
                        text = p.get_text(" ", strip=True)
                        if (
                            text
                            and len(text) > 10
                            and not text.lower().startswith("copyright")
                        ):
                            content_paragraphs.append(text)

            if not content_paragraphs:
                paragraphs = soup.select("article p, .content p, .main-content p")
                for p in paragraphs:
                    text = p.get_text(" ", strip=True)
                    if (
                        text
                        and len(text) > 10
                        and not text.lower().startswith("copyright")
                    ):
                        content_paragraphs.append(text)

            raw_content = " ".join(content_paragraphs)
            content = cls.clean_content(raw_content)

            # Skip articles with too little content - likely broken/paywalled/junk parse
            if len(content) < 200:
                logger.debug(
                    f"Content too short ({len(content)} chars) for {url}, skipping"
                )
                return None

            build_date = datetime.now(timezone.utc)
            genre = cls.extract_genre_from_url(url)

            article = {
                "id": url,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": build_date,
                "title": title,
                "authors": author,
                "language": "en-us",
                "source": cls.SOURCE,
                "content": content,
                "genre": genre,
                "media_origin": "International",
                "tags": [],
                "url": url,
            }

            logger.debug(f"Successfully parsed: {title[:50]}... ({len(content)} chars)")
            return article

        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = SPORTS_TABLE
        """Main pipeline to run the Sportsnet scraper."""
        try:
            logger.info("Starting Sportsnet scraper pipeline...")

            article_links = SportsnetScraper.fetch_article_links()

            if not article_links:
                logger.warning("No article links found")
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                    "error": "No articles found",
                }

            logger.info(
                f"Found {len(article_links)} article links. Fetching articles..."
            )

            all_articles = []
            failed_count = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(SportsnetScraper.fetch_article, url): url
                    for url in article_links
                }

                for future in concurrent.futures.as_completed(futures):
                    url = futures[future]
                    try:
                        article = future.result()
                        if article:
                            all_articles.append(article)
                        else:
                            failed_count += 1
                    except Exception as e:
                        logger.error(f"Error processing {url}: {e}")
                        failed_count += 1

                    if len(all_articles) % 10 == 0 and len(all_articles) > 0:
                        logger.info(f"Fetched {len(all_articles)} articles so far...")

            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"Successfully fetched {len(all_articles)} unique articles. Failed: {failed_count}"
            )

            if all_articles:
                sample = all_articles[0]
                logger.info(
                    f"Sample - Title: {sample['title'][:50]}..., Genre: {sample['genre']}, Content length: {len(sample['content'])}"
                )

                result = SupabaseClient.insert_articles(
                    all_articles, table_name=target_table, category="sports"
                )
                logger.info(f"Inserted {result.get('inserted_count', 0)} articles")
                return result
            else:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                    "error": "No articles fetched successfully",
                }

        except Exception as e:
            logger.error(f"Sportsnet pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
