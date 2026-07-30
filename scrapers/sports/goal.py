import json
import uuid
import logging
import concurrent.futures
from datetime import datetime, timezone

import cloudscraper
from bs4 import BeautifulSoup

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient
from config import SPORTS_TABLE

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
        # Skip betting/odds promo links that return 403 or non-articles
        if any(
            x in url.lower() for x in ["/betting/", "/odds/", "/gambling/", "/promos/"]
        ):
            return None

        try:
            response = scraper.get(
                url,
                headers=get_random_headers(),
                timeout=10,
            )
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            title_tag = (
                soup.select_one("h1[data-testid='article-title']")
                or soup.select_one("h1.article_title")
                or soup.select_one("h1")
            )

            title = title_tag.get_text(strip=True) if title_tag else ""

            author_nodes = (
                soup.select("span[data-testid='author-link'] a")
                or soup.select(".author a")
                or soup.select("a[href*='/author/']")
            )

            authors = [
                a.get_text(strip=True) for a in author_nodes if a.get_text(strip=True)
            ]

            authors = ", ".join(authors) if authors else "Goal Staff"

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

            tags = [
                t.get_text(strip=True)
                for t in soup.select(
                    ".fco-tag-button-text, .tag-list span, a[aria-label='tag button'] span"
                )
                if t.get_text(strip=True)
            ]

            body = (
                soup.select_one("article[data-testid='article-body']")
                or soup.select_one("div.article-body_body__ASOmp")
                or soup.select_one("article")
            )

            if not body:
                return None

            for el in body.select(
                "script, style, iframe, noscript, table, svg, .fco-affiliate-button"
            ):
                el.decompose()

            paragraphs = []

            for p in body.select("p"):
                text = p.get_text(" ", strip=True)
                if len(text) > 30 and "cookie" not in text.lower():
                    paragraphs.append(text)

            content = "\n\n".join(paragraphs)

            if len(content) < 200:
                return None

            return {
                "title": title,
                "authors": authors,
                "content": content,
                "tags": tags,
                "pub_date": pub_date,
            }

        except Exception:
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
                    timeout=15,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.content, "xml")
                urls = soup.find_all("url")

                feed_build_date = datetime.now(timezone.utc)
                article_tasks = []

                for item in urls:
                    loc = item.find("loc")
                    if not loc:
                        continue
                    url_str = loc.text.strip()

                    # Filter betting links early
                    if any(
                        x in url_str.lower()
                        for x in ["/betting/", "/odds/", "/gambling/", "/promos/"]
                    ):
                        continue

                    title_tag = item.find("news:title")
                    image_tag = item.find("image:loc")
                    date_tag = item.find("news:publication_date")
                    keywords_tag = item.find("news:keywords")

                    article_tasks.append(
                        {
                            "url": url_str,
                            "title_fallback": (
                                title_tag.text.strip() if title_tag else ""
                            ),
                            "image_url": (
                                image_tag.text.strip()
                                if image_tag and image_tag.text
                                else None
                            ),
                            "date_fallback": (
                                GoalSportsNewsPipeline.parse_date(date_tag.text.strip())
                                if date_tag
                                else datetime.now(timezone.utc)
                            ),
                            "tags_fallback": (
                                [
                                    t.strip()
                                    for t in keywords_tag.text.split(",")
                                    if t.strip()
                                ]
                                if keywords_tag and keywords_tag.text
                                else []
                            ),
                        }
                    )

                # Limit to 50 articles per run for fast execution
                article_tasks = article_tasks[:50]

                articles = []

                def _process_task(task):
                    article_data = GoalSportsNewsPipeline.fetch_article(
                        scraper, task["url"]
                    )
                    if not article_data:
                        return None
                    return {
                        "id": task["url"],
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_data.get("pub_date")
                        or task["date_fallback"],
                        "feedBuildDate": feed_build_date,
                        "title": article_data["title"] or task["title_fallback"],
                        "authors": article_data["authors"],
                        "language": "en-US",
                        "image": task["image_url"],
                        "source": GoalSportsNewsPipeline.SOURCE,
                        "content": article_data["content"],
                        "genre": "Sports",
                        "media_origin": "international",
                        "tags": article_data["tags"] or task["tags_fallback"],
                    }

                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [
                        executor.submit(_process_task, task) for task in article_tasks
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        res = future.result()
                        if res:
                            articles.append(res)

                logger.info(f"Parsed {len(articles)} Goal articles")
                return articles

        except Exception as e:
            logger.error(f"Goal sitemap fetch failed: {e}")
            return []

    # -----------------------------
    # PIPELINE RUNNER
    # -----------------------------
    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        try:
            target_table = SPORTS_TABLE
            all_articles = []

            for sitemap in GoalSportsNewsPipeline.SITEMAPS:
                articles = GoalSportsNewsPipeline.fetch_sitemap(sitemap)
                all_articles.extend(articles)

            if not all_articles:
                return {
                    "inserted_count": 0,
                    "total_articles": 0,
                }

            return SupabaseClient.insert_articles(all_articles, table_name=target_table, category="sports")

        except Exception as e:
            logger.error(f"Goal pipeline failed: {e}")

            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
