import uuid
import logging
from datetime import datetime, timezone

import cloudscraper
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class NYTSoccerRSSPipeline:
    """
    NYTimes / The Athletic Soccer RSS Pipeline
    """

    SOURCE = "NYTimes - The Athletic"

    FEEDS = [
        "https://www.nytimes.com/athletic/rss/soccer/",
    ]

    # -----------------------------
    # DATE PARSER
    # -----------------------------
    @staticmethod
    def parse_date(date_str):
        try:
            if not date_str:
                return datetime.now(timezone.utc)

            return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").replace(
                tzinfo=timezone.utc
            )

        except Exception:
            return datetime.now(timezone.utc)

    # -----------------------------
    # ARTICLE SCRAPER (FULL CONTENT)
    # -----------------------------
    @staticmethod
    def fetch_article(scraper, url):
        try:
            response = scraper.get(
                url,
                headers=get_random_headers(),
                timeout=30,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # -----------------------------
            # TITLE (stable)
            # -----------------------------
            title_tag = soup.select_one("h1")
            title = title_tag.get_text(strip=True) if title_tag else ""

            # -----------------------------
            # ARTICLE BODY (ROBUST STRATEGY)
            # -----------------------------

            body = (
                soup.select_one("#article-container-grid .article-content-container")
                or soup.select_one("#article-container-grid")
                or soup.find("article")
                or soup.body
            )

            if not body:
                return {"title": title, "content": ""}

            # Remove noise
            for el in body.select(
                "script, style, noscript, iframe, nav, footer, aside, form, button"
            ):
                el.decompose()

            # Extract paragraphs (strict filtering)
            paragraphs = []
            for p in body.find_all("p"):
                text = p.get_text(" ", strip=True)

                # filter junk / nav / captions
                if len(text) < 20:
                    continue
                if "advertisement" in text.lower():
                    continue

                paragraphs.append(text)

            content = "\n\n".join(paragraphs)

            author_container = soup.select_one("#articleByLineString")

            authors = []
            if author_container:
                authors = [a.get_text(strip=True) for a in author_container.select("a")]

            authors_str = ", ".join(authors) if authors else "The Athletic Staff"

            return {"title": title, "content": content, "authors": authors_str}

        except Exception as e:
            logger.warning(f"Article fetch failed: {url} | {e}")
            return None

    # -----------------------------
    # RSS PARSER
    # -----------------------------
    @staticmethod
    def fetch_feed(feed_url):
        try:
            logger.info(f"Fetching feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    headers=get_random_headers(),
                    timeout=30,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.content, "xml")

                feed_build_date = datetime.now(timezone.utc)

                items = soup.find_all("item")
                articles = []

                for item in items:
                    try:
                        title = item.find("title").text if item.find("title") else ""

                        link = item.find("link").text if item.find("link") else None
                        if not link:
                            continue

                        guid = item.find("guid").text if item.find("guid") else link

                        pub_date_raw = (
                            item.find("pubDate").text if item.find("pubDate") else ""
                        )
                        pub_date = NYTSoccerRSSPipeline.parse_date(pub_date_raw)

                        # media image
                        media = item.find("media:content")
                        image = (
                            media["url"] if media and media.has_attr("url") else None
                        )

                        # -----------------------------
                        # 🔥 FETCH FULL ARTICLE CONTENT
                        # -----------------------------
                        full_article = NYTSoccerRSSPipeline.fetch_article(scraper, link)
                        authors_str = (
                            full_article["authors"]
                            if full_article and full_article.get("authors")
                            else "The Athletic Staff"
                        )

                        content = ""
                        if full_article and full_article.get("content"):
                            content = full_article["content"]
                        else:
                            # fallback to RSS description
                            desc = item.find("description")
                            content = desc.text if desc else ""

                        if len(content) < 200:
                            logger.info(f"Content too short, skipping: {link}")
                            continue

                        logger.info(f"Parsed article: {title}")

                        articles.append(
                            {
                                "id": guid,
                                "article_id": str(uuid.uuid4()),
                                "articlePubDate": pub_date,
                                "feedBuildDate": feed_build_date,
                                "title": title,
                                "authors": authors_str,
                                "language": "en-US",
                                "image": image,
                                "source": NYTSoccerRSSPipeline.SOURCE,
                                "content": content,
                                "genre": "Sports",
                                "media_origin": "international",
                                "tags": [],
                                "url": link,
                            }
                        )

                    except Exception as e:
                        logger.warning(f"Item parse failed: {e}")
                        continue

                logger.info(f"Parsed {len(articles)} articles from NYT RSS")
                return articles

        except Exception as e:
            logger.error(f"Feed fetch failed: {e}")
            return []

    # -----------------------------
    # RUNNER
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for feed in NYTSoccerRSSPipeline.FEEDS:
                articles = NYTSoccerRSSPipeline.fetch_feed(feed)
                all_articles.extend(articles)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            return SupabaseClient.insert_articles(all_articles)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e)}
