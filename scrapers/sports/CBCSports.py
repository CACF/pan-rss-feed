import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.utilities import get_random_headers
import concurrent.futures
import requests
from urllib.parse import urljoin

from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class CBCSportsScraper:
    SOURCE = "CBC Sports"
    BASE_URL = "https://www.cbc.ca/sports"

    HEADERS = {
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    @staticmethod
    def parse_date(date_str: str):
        """Parse CBC Sports date strings to datetime."""
        try:
            # Try parsing ISO format first
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

            # Try parsing common CBC date formats
            try:
                import dateparser

                dt = dateparser.parse(date_str, settings={"TIMEZONE": "UTC"})
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
            except ImportError:
                pass

            # Fallback to current time
            logger.warning(f"Could not parse date '{date_str}', using current time")
            return datetime.now(timezone.utc)

        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(html):
        """
        Clean article HTML:
        - Remove scripts, styles, and asides
        - Unwrap <a> tags (keep text, remove links)
        - Remove visible URLs (http, https, www)
        - Normalize spaces
        """
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "aside", "iframe", "noscript"]):
                tag.decompose()
            for a in soup.find_all("a"):
                a.unwrap()
            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"clean_content error: {e}")
            return html

    @classmethod
    def fetch_genre_pages(cls):
        """Get all sports genre pages from the main sports page."""
        try:
            genre_urls = []
            logger.info(f"Fetching genre pages from: {cls.BASE_URL}")

            resp = requests.get(
                cls.BASE_URL, timeout=30, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")

            # Try multiple selectors to find genre/sports pages
            selectors = [
                "ul.seriesList > li.seriesListItem a",
                ".seriesList a",
                "[data-cy='series-list'] a",
                ".sports-nav a",
                ".category-nav a",
                "nav a[href*='/sports/']",
                "a[href*='/sports/']:not([href*='#'])",
            ]

            found_links = []
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    logger.info(
                        f"Found {len(elements)} links with selector: {selector}"
                    )
                    found_links.extend(elements)

            # If no links found with specific selectors, try to find all relevant links
            if not found_links:
                logger.warning(
                    "No genre links found with specific selectors, trying to find all sports links..."
                )
                all_links = soup.find_all("a", href=True)
                for link in all_links:
                    href = link.get("href", "")
                    # Look for sports-related links
                    if (
                        "/sports/" in href
                        and not href.startswith("#")
                        and not href.startswith("javascript:")
                    ):
                        # Filter out common non-genre pages
                        if not any(
                            x in href.lower()
                            for x in ["/live/", "/video/", "/news/", "/radio/"]
                        ):
                            found_links.append(link)

            # Process found links
            for link_elem in found_links:
                href = link_elem.get("href")
                if href:
                    # Make URL absolute
                    if href.startswith("/"):
                        href = "https://www.cbc.ca" + href
                    elif not href.startswith("http"):
                        href = urljoin(cls.BASE_URL, href)

                    # Avoid duplicates and exclude homepage
                    if (
                        href not in genre_urls
                        and href != cls.BASE_URL
                        and href != "https://www.cbc.ca/sports/"
                    ):
                        genre_urls.append(href)
                        logger.info(f"Found genre page: {href}")

            # If still no genre pages found, use the main sports page itself
            if not genre_urls:
                logger.warning("No genre pages found, using main sports page")
                genre_urls.append(cls.BASE_URL + "/all")

            logger.info(f"Found {len(genre_urls)} genre pages")
            return genre_urls

        except Exception as e:
            logger.error(f"Failed to fetch genre pages: {e}")
            # Fallback to main sports page
            return [cls.BASE_URL + "/all"]

    @classmethod
    def fetch_article_links_from_genre(cls, genre_url):
        """Get article links from a specific genre page."""
        try:
            links = []
            logger.info(f"Fetching article links from genre page: {genre_url}")

            resp = requests.get(
                genre_url, timeout=30, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")

            # Try multiple selectors to find article links
            selectors = [
                '[data-cy="cwq-card-grid"] a[data-cy="type-story"]',
                'a[data-cy="type-story"]',
                '.card a[href*="/sports/"]',
                ".story-card a",
                ".feature-card a",
                'article a[href*="/sports/"]',
                '.content a[href*="/sports/"]',
            ]

            found_links = []
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    logger.info(
                        f"Found {len(elements)} article links with selector: {selector}"
                    )
                    found_links.extend(elements)

            # Process found links
            for a in found_links:
                href = a.get("href")
                if href:
                    # Make URL absolute
                    if href.startswith("/"):
                        href = "https://www.cbc.ca" + href
                    elif not href.startswith("http"):
                        href = urljoin(genre_url, href)

                    # Filter out non-article links
                    if (
                        href
                        and "/sports/" in href
                        and not any(
                            x in href.lower()
                            for x in [
                                "/live/",
                                "/video/",
                                "/author/",
                                "/shows/",
                                "/schedule",
                                "/tv-schedule",
                                "/features/",
                                ".jpg",
                                ".png",
                            ]
                        )
                    ):
                        links.append(href)
                        logger.debug(f"Found article link: {href}")

            # If no links found, try to find all article links on the page
            if not links:
                logger.debug(
                    f"No links found with specific selectors for {genre_url}, trying to find all article links..."
                )
                all_links = soup.find_all("a", href=True)
                for link in all_links:
                    href = link.get("href", "")
                    if "/sports/" in href and not href.startswith("#"):
                        # Check if it's likely an article link
                        if not any(
                            x in href.lower()
                            for x in [
                                "/live/",
                                "/video/",
                                "/radio/",
                                "/news/",
                                "/author/",
                                "/shows/",
                                "/schedule",
                                ".jpg",
                            ]
                        ):
                            if href.startswith("/"):
                                href = "https://www.cbc.ca" + href
                            elif not href.startswith("http"):
                                href = urljoin(genre_url, href)
                            links.append(href)

            # Deduplicate links
            links = list(set(links))
            logger.info(f"Found {len(links)} article links from {genre_url}")
            return links

        except Exception as e:
            logger.error(f"Failed to fetch article links from genre {genre_url}: {e}")
            return []

    @classmethod
    def fetch_article(cls, url):
        """Fetch and parse an individual CBC Sports article."""
        if any(
            x in url.lower()
            for x in ["/author/", "/shows/", "/schedule", "/features/", ".jpg", ".png"]
        ):
            return None
        try:
            logger.debug(f"Fetching article: {url}")

            resp = requests.get(
                url, timeout=15, headers=get_random_headers(cls.HEADERS)
            )
            try:
                resp.raise_for_status()
                payload = resp.content
            finally:
                resp.close()

            soup = BeautifulSoup(payload, "html.parser")

            # Extract title from h1.detailHeadline
            title_elem = soup.select_one("h1.detailHeadline") or soup.select_one("h1")
            title = title_elem.get_text(strip=True) if title_elem else ""

            if not title:
                logger.debug(f"No title found for {url}")
                return None

            # Extract publication date
            time_elem = soup.select_one(".bylineDetails time.timeStamp")
            pub_date = datetime.now(timezone.utc)
            if time_elem:
                date_str = time_elem.get("datetime") or time_elem.get_text(strip=True)
                if date_str:
                    pub_date = cls.parse_date(date_str)

            # Extract author
            author_elem = soup.select_one(".bylineDetails .authorText a")
            author = author_elem.get_text(strip=True) if author_elem else "CBC Sports"

            # Extract content from div.story > p
            content_paragraphs = []

            # Primary selector: div.story > p
            story_div = soup.select_one("div.story")
            if story_div:
                paragraphs = story_div.select("p")
                for p in paragraphs:
                    text = p.get_text(" ", strip=True)
                    # Skip empty, very short, or copyright paragraphs
                    if (
                        text
                        and len(text) > 10
                        and not text.lower().startswith("copyright")
                    ):
                        content_paragraphs.append(text)

            # Fallback: try other common content containers
            if not content_paragraphs:
                logger.debug(
                    f"No content found with primary selector for {url}, trying fallbacks..."
                )

                # Try .story-content
                story_content = soup.select_one(".story-content")
                if story_content:
                    paragraphs = story_content.select("p")
                    for p in paragraphs:
                        text = p.get_text(" ", strip=True)
                        if (
                            text
                            and len(text) > 10
                            and not text.lower().startswith("copyright")
                        ):
                            content_paragraphs.append(text)

                # If still no content, try any paragraph within article
                if not content_paragraphs:
                    article_content = soup.select(
                        "article p, .content p, .main-content p"
                    )
                    for p in article_content:
                        text = p.get_text(" ", strip=True)
                        if (
                            text
                            and len(text) > 10
                            and not text.lower().startswith("copyright")
                        ):
                            content_paragraphs.append(text)

            raw_content = " ".join(content_paragraphs)
            content = cls.clean_content(raw_content)

            # Skip if content is too short (might be a video or gallery page)
            if len(content) < 50:
                logger.debug(
                    f"Content too short ({len(content)} chars) for {url}, might not be an article"
                )
                content = content if content else "Content not available"

            build_date = datetime.now(timezone.utc)

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
                "genre": "Sports",
                "media_origin": "International",
                "tags": [],
                "url": url,
            }

            logger.debug(
                f"Successfully parsed article: {title[:50]}... ({len(content)} chars)"
            )
            return article

        except Exception as e:
            logger.warning(f"Failed to fetch CBC Sports article {url}: {e}")
            return None

    @classmethod
    def fetch_all_articles(cls):
        """Fetch all articles from all genre pages."""
        try:
            # Step 1: Get all genre pages
            genre_pages = cls.fetch_genre_pages()

            if not genre_pages:
                logger.warning("No genre pages found")
                return []

            logger.info(
                f"Found {len(genre_pages)} genre pages. Fetching articles from each..."
            )

            all_article_links = []

            # Step 2: Get article links from each genre page
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(
                        cls.fetch_article_links_from_genre, genre_url
                    ): genre_url
                    for genre_url in genre_pages
                }

                for future in concurrent.futures.as_completed(futures):
                    genre_url = futures[future]
                    try:
                        links = future.result()
                        all_article_links.extend(links)
                        logger.info(f"Added {len(links)} articles from {genre_url}")
                    except Exception as e:
                        logger.error(f"Error fetching articles from {genre_url}: {e}")

            # Deduplicate article links
            all_article_links = list(set(all_article_links))
            logger.info(f"Total unique article links found: {len(all_article_links)}")

            return all_article_links

        except Exception as e:
            logger.error(f"Failed to fetch all articles: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = table_name or SPORTS_TABLE
        """Main pipeline to run the CBC Sports scraper."""
        from config import SPORTS_TABLE

        try:
            logger.info("Starting CBC Sports scraper pipeline...")

            # Step 1: Get all article links from all genre pages
            article_links = CBCSportsScraper.fetch_all_articles()

            if not article_links:
                logger.warning("No article links found")
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                    "error": "No articles found",
                }

            logger.info(
                f"Found {len(article_links)} article links. Starting to fetch articles..."
            )

            all_articles = []
            failed_count = 0

            # Step 2: Fetch each article concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(CBCSportsScraper.fetch_article, url): url
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

                    # Log progress every 10 articles
                    if len(all_articles) % 10 == 0 and len(all_articles) > 0:
                        logger.info(f"Fetched {len(all_articles)} articles so far...")

            # Deduplicate by URL
            all_articles = list(
                {article["id"]: article for article in all_articles}.values()
            )

            logger.info(
                f"Successfully fetched {len(all_articles)} unique articles. "
                f"Failed: {failed_count}"
            )

            # Log sample article for debugging
            if all_articles:
                sample = all_articles[0]
                logger.info(
                    f"Sample article - Title: {sample['title'][:50]}..., Content length: {len(sample['content'])}"
                )
            else:
                logger.warning("No articles were successfully fetched")
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                    "error": "No articles fetched successfully",
                }

            # Step 3: Insert into database
            result = SupabaseClient.insert_articles(
                all_articles, table_name=target_table
            )
            logger.info(
                f"Inserted {result.get('inserted_count', 0)} articles into database"
            )
            return result

        except Exception as e:
            logger.error(f"CBC Sports pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
