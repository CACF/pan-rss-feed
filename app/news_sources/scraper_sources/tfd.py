import random
import uuid
import logging
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.utilities import MongoDBClient, get_random_headers
import concurrent.futures
import requests

logger = logging.getLogger(__name__)


class FinancialDailyBusinessPipeline:
    """
    Financial Daily Business scraper pipeline that fetches, parses,
    and stores business news articles from thefinancialdaily.com.
    """
    SOURCE = "Financial Daily - Business"
    BASE_URL = "https://thefinancialdaily.com"
    SECTION_URL = f"{BASE_URL}/category/business/"

    @staticmethod
    def parse_date(date_str):
        """Parse article date string into timezone-aware datetime."""
        try:
            try:
                return datetime.fromisoformat(date_str)
            except ValueError:
                pass

            for fmt in ("%B %d, %Y", "%Y-%m-%d", "%d %B %Y"):
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

            return datetime.now(timezone.utc)
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        """Clean HTML content to plain text and remove links."""
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style", "form", "aside", "iframe", "noscript"]):
                tag.decompose()
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()
            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r"http\S+|www\.\S+", "", text)
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html or ""

    @classmethod
    def fetch_article_links(cls):
        """Fetch links to individual business articles, excluding irrelevant sections."""
        try:
            logger.info(f"Fetching Financial Daily section: {cls.SECTION_URL}")
            response = requests.get(cls.SECTION_URL, timeout=30, headers=get_random_headers())
            try:
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()

            soup = BeautifulSoup(payload, "html.parser")

            links = []
            blogs = soup.select("h3.entry-title.td-module-title a")
            skip_section = soup.select(
                "div.vc_column_inner.tdi_104.wpb_column.vc_column_container.tdc-inner-column.td-pb-span12 h3.td-module-title a"
            )
            skip_links = {a.get("href") for a in skip_section if a.get("href")}
            for a in blogs:
                href = a.get("href")
                if href and href not in skip_links:
                    links.append(href)

            logger.info(f"Found {len(links)} valid business article links (after skipping unwanted section).")
            return links

        except Exception as e:
            logger.error(f"Failed to fetch article links: {e}")
            return []

    @staticmethod
    def extract_description(soup):
        """Extract article description/content from article page."""
        try:
            content_div = soup.select_one("div.tdb-block-inner.td-fix-index p")
            if not content_div:
                return ""

            for tag in content_div(["script", "style", "form", "aside", "iframe", "noscript"]):
                tag.decompose()
            for a_tag in content_div.find_all("a"):
                a_tag.unwrap()

            text = content_div.get_text(separator=" ", strip=True)
            text = re.sub(r"http\S+|www\.\S+", "", text)
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"Failed to extract description: {e}")
            return ""

    @classmethod
    def fetch_article(cls, url):
        """Fetch and parse a single article page."""
        try:
            response = requests.get(url, timeout=30, headers=get_random_headers())
            try:
                response.raise_for_status()
                payload = response.content
            finally:
                response.close()
            soup = BeautifulSoup(payload, "html.parser")
            title_elem = soup.select_one("h1.tdb-title-text")
            title = title_elem.get_text(strip=True) if title_elem else ""
            date_elem = soup.select_one(
                "div.tdb-block-inner.td-fix-index > time.entry-date.updated"
            )
            date_str = date_elem["datetime"] if date_elem and date_elem.has_attr("datetime") else ""
            pub_date = cls.parse_date(date_str) if date_str else datetime.now(timezone.utc)
            author_elem = soup.select_one("div.tdb-author-name-wrap a.tdb-author-name")
            author = author_elem.get_text(strip=True) if author_elem else "Unknown"
            content = cls.extract_description(soup)
            if len(content) < 200:
                logger.info(f"Skipping short article ({len(content)} chars): {url}")
                return None

            article = {
                "_id": url,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": datetime.now(timezone.utc),
                "title": title,
                "authors": author,
                "language": "en-us",
                "source": cls.SOURCE,
                "content": content,
                "genre": "Business",
                "media_origin": "domestic",
                "tags": [],
            }

            return article
        except Exception as e:
            logger.warning(f"Failed to fetch article {url}: {e}")
            return None

    @classmethod
    def run_pipeline(cls, input_data=None):
        """Run the scraper pipeline."""
        try:
            all_articles = []
            article_links = cls.fetch_article_links()
            max_workers = min(8, len(article_links)) or 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(cls.fetch_article, link): link for link in article_links}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        article = future.result()
                        if article:
                            all_articles.append(article)
                    except Exception as e:
                        logger.warning(f"Error fetching article in pool: {e}")

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            result = MongoDBClient.insert_articles_to_mongo(
                all_articles,
                user_email=(input_data.get("email") if input_data else None),
            )
            return result

        except Exception as e:
            logger.error(f"Financial Daily pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
