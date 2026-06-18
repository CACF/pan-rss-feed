import json
import uuid
import logging
from datetime import datetime, timezone

import cloudscraper
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class GoalSportsNewsPipeline:
    """
    Goal.com Google News Sitemap Pipeline
    """

    SOURCE = "Goal"

    SITEMAPS = [
        "https://www.goal.com/en/sitemap/google-news.xml",
    ]

    # -----------------------------
    # DATE PARSER
    # -----------------------------
    @staticmethod
    def parse_date(date_str):
        try:
            if not date_str:
                return datetime.now(timezone.utc)

            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(
                timezone.utc
            )

        except Exception:
            return datetime.now(timezone.utc)

    # -----------------------------
    # ARTICLE SCRAPER (DOM-BASED)
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

            # =====================================================
            # TITLE (BEST → fallback chain)
            # =====================================================
            title_tag = (
                soup.select_one("h1[data-testid='article-title']")
                or soup.select_one("h1.article_title")
                or soup.select_one("h1")
            )

            title = title_tag.get_text(strip=True) if title_tag else ""

            # =====================================================
            # AUTHORS (stable + fallback)
            # =====================================================
            author_nodes = (
                soup.select("span[data-testid='author-link'] a")
                or soup.select(".author a")
                or soup.select("a[href*='/author/']")
            )

            authors = [
                a.get_text(strip=True) for a in author_nodes if a.get_text(strip=True)
            ]

            authors = ", ".join(authors) if authors else "Goal Staff"

            # =====================================================
            # DATE (BEST SELECTOR)
            # =====================================================
            date_tag = (
                soup.select_one("time[data-testid='publish-time']")
                or soup.select_one("time[datetime]")
                or soup.find("time")
            )

            pub_date = GoalSportsNewsPipeline.parse_date(
                date_tag.get("datetime")
                if date_tag and date_tag.has_attr("datetime")
                else ""
            )

            # =====================================================
            # TAGS (VERY IMPORTANT FIX)
            # Goal uses multiple tag structures
            # =====================================================
            tags = [
                t.get_text(strip=True)
                for t in soup.select(
                    ".fco-tag-button-text, .tag-list span, a[aria-label='tag button'] span"
                )
                if t.get_text(strip=True)
            ]

            # =====================================================
            # CONTENT (CRITICAL FIX - AVOID HASH CLASS DEPENDENCY)
            # =====================================================
            body = (
                soup.select_one("article[data-testid='article-body']")
                or soup.select_one("div.article-body_body__ASOmp")
                or soup.select_one("article")
            )

            if not body:
                logger.warning(f"No body found: {url}")
                return None

            # remove junk
            for el in body.select(
                "script, style, iframe, noscript, table, svg, .fco-affiliate-button"
            ):
                el.decompose()

            paragraphs = []

            for p in body.select("p"):
                text = p.get_text(" ", strip=True)

                # filter noise + UI text
                if len(text) > 30 and "cookie" not in text.lower():
                    paragraphs.append(text)

            content = "\n\n".join(paragraphs)

            if len(content) < 200:
                logger.info(f"Skipped short article: {url}")
                return None

            return {
                "title": title,
                "authors": authors,
                "content": content,
                "tags": tags,
                "pub_date": pub_date,
            }

        except Exception as e:
            logger.warning(f"Failed article fetch: {url} | {e}")
            return None

    # -----------------------------
    # SITEMAP PARSER
    # -----------------------------
    @staticmethod
    def fetch_sitemap(sitemap_url):
        try:
            logger.info(f"Fetching Goal sitemap: {sitemap_url}")

            with cloudscraper.create_scraper() as scraper:

                response = scraper.get(
                    sitemap_url,
                    headers=get_random_headers(),
                    timeout=30,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.content, "xml")
                urls = soup.find_all("url")

                feed_build_date = datetime.now(timezone.utc)
                articles = []
                for item in urls:
                    try:
                        loc = item.find("loc")
                        if not loc:
                            continue

                        article_url = loc.text.strip()

                        article_data = GoalSportsNewsPipeline.fetch_article(
                            scraper,
                            article_url,
                        )

                        if not article_data:
                            continue

                        # fallback title (if scraper returns empty)
                        title_tag = item.find("news:title")
                        title = article_data["title"] or (
                            title_tag.text.strip() if title_tag else ""
                        )

                        image_url = None
                        image = item.find("image:loc")
                        if image and image.text:
                            image_url = image.text.strip()

                        # fallback date
                        date_tag = item.find("news:publication_date")
                        pub_date = article_data.get("pub_date") or (
                            GoalSportsNewsPipeline.parse_date(date_tag.text.strip())
                            if date_tag
                            else datetime.now(timezone.utc)
                        )

                        # fallback keywords from sitemap
                        keywords_tag = item.find("news:keywords")
                        tags = article_data["tags"]

                        if keywords_tag and keywords_tag.text:
                            tags = [
                                t.strip()
                                for t in keywords_tag.text.split(",")
                                if t.strip()
                            ]

                        article = {
                            "id": article_url,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "authors": article_data["authors"],
                            "language": "en-US",
                            "image": image_url,
                            "source": GoalSportsNewsPipeline.SOURCE,
                            "content": article_data["content"],
                            "genre": "Sports",
                            "media_origin": "international",
                            "tags": tags,
                        }

                        articles.append(article)

                    except Exception as e:
                        logger.warning(f"Failed sitemap item: {e}")
                        continue

                logger.info(f"Parsed {len(articles)} Goal articles")
                return articles

        except Exception as e:
            logger.error(f"Goal sitemap fetch failed: {e}")
            return []

    # -----------------------------
    # PIPELINE RUNNER
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            for sitemap in GoalSportsNewsPipeline.SITEMAPS:
                articles = GoalSportsNewsPipeline.fetch_sitemap(sitemap)
                all_articles.extend(articles)

            if not all_articles:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                }

            return SupabaseClient.insert_articles(all_articles)

        except Exception as e:
            logger.error(f"Goal pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
