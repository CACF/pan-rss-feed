import re
import uuid
import logging
from datetime import datetime, timezone
import time
from urllib.parse import urlparse

from config import WAFAQ_TABLE
from app.utils.supabase_client import SupabaseClient

from scrapers.shared.article_parser import (
    parse_date,
    clean_text,
    full_description,
    resolve_author,
    resolve_source,
    resolve_google_news_link,
)
from scrapers.shared.http_client import fetch_rss_feed

from .skip_domains import SKIP_DOMAINS
from .keywords import ISLAMABAD_KEYWORDS, EMBASSY_KEYWORDS
from .feeds import GOOGLE_NEWS_FEEDS, GENERAL_NATIONAL_FEEDS, EMBASSY_NEWS_FEEDS

logger = logging.getLogger(__name__)


class IslamabadRSSPipeline:
    SOURCE = "Google News"
    MAX_WORKERS = 3

    @staticmethod
    def is_islamabad_related(text):
        lower = text.lower()
        return any(kw in lower for kw in ISLAMABAD_KEYWORDS)

    @staticmethod
    def is_embassy_related(text):
        lower = text.lower()
        return any(kw.lower() in lower for kw in EMBASSY_KEYWORDS)

    @staticmethod
    def process_item(
        item,
        feed_build_date,
        is_google_news=False,
        apply_islamabad_filter=True,
        is_embassy_feed=False,
        **kwargs
    ):
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pubdate_elem = item.find("articlePubDate")

            if not title_elem or not link_elem:
                return None

            title = title_elem.get_text(strip=True)
            
            if apply_islamabad_filter:
                if not IslamabadRSSPipeline.is_islamabad_related(title):
                    logger.debug(f"Not Islamabad Keywords Related, using raw link: '{title}'")
                    return None
            
            # if is_embassy_feed:
            #     if not IslamabadRSSPipeline.is_embassy_related(title):
            #         logger.debug(f"Not Embassy Keywords Related, using raw link: '{title}'")
            #         return None

            raw_link = link_elem.get_text(strip=True)

            if is_google_news:
                link = resolve_google_news_link(raw_link)
                if not link:
                    logger.debug(f"Could not resolve Google News link, using raw link: '{title}'")
                    link = raw_link
            else:
                link = raw_link

            domain = urlparse(link).netloc.lower()
            if domain in SKIP_DOMAINS:
                logger.debug(f"Skipping article from {domain}: '{title}'")
                return None

            rss_pub_date = (
                parse_date(pubdate_elem.get_text()) if pubdate_elem else None
            ) or datetime.now(timezone.utc)

            source = resolve_source(item, IslamabadRSSPipeline.SOURCE)
            logger.info(f"Processing: '{title}' | source={source} | link={link}")

            full = full_description(link)
            author = full["author"] or resolve_author(item, title)

            rss_categories = [
                c.get_text(strip=True)
                for c in item.find_all("category")
                if c.get_text(strip=True)
            ]

            content_elem = item.find("content:encoded") or item.find("description")
            content_raw = content_elem.get_text() if content_elem else ""
            content = clean_text(content_raw)

            if full["content"] and len(full["content"]) > len(content):
                content = full["content"]

            image_url = full["image"]
            pub_date = full["published"] or rss_pub_date

            seen = set()
            categories = []
            for tag in rss_categories + full["tags"]:
                key = tag.lower()
                if key not in seen:
                    seen.add(key)
                    categories.append(tag)

            if len(content) < 200:
                logger.debug(f"Skipped (too short after full fetch): '{title}'")
                return None

            if apply_islamabad_filter:
                if not IslamabadRSSPipeline.is_islamabad_related(title + " " + content):
                    logger.debug(f"Not Islamabad-related (post-fetch), using raw link: '{title}'")
                    return None

            article = {
                "id": link,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": feed_build_date,
                "title": title,
                "authors": author,
                "language": "en-US",
                "image": image_url,
                "source": source,
                "content": content,
                "genre": "Embassy News" if is_embassy_feed else "General News",
                "media_origin": "local",
                "tags": categories,
            }

            logger.info(
                f"Added: '{title}' | {len(content)} chars | "
                f"author={author} | tags={len(categories)} | "
                f"image={'yes' if image_url else 'no'} | "
                f"date={'article' if full['published'] else 'rss-fallback'}"
            )

            return article

        except Exception as e:
            logger.warning(f"Failed parsing item: {e}")
            return None

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = table_name or WAFAQ_TABLE
        try:
            all_articles = []

            logger.info("── Islamabad, Pakistan — Google News ──")
            for feed_url in GOOGLE_NEWS_FEEDS:
                all_articles.extend(
                    fetch_rss_feed(
                        feed_url=feed_url,
                        process_item_func=IslamabadRSSPipeline.process_item,
                        max_workers=IslamabadRSSPipeline.MAX_WORKERS,
                        is_google_news=True,
                        apply_islamabad_filter=True,
                    )
                )
                time.sleep(10)

            logger.info("── Islamabad, Pakistan — General National Feeds ──")
            for feed_url in GENERAL_NATIONAL_FEEDS:
                all_articles.extend(
                    fetch_rss_feed(
                        feed_url=feed_url,
                        process_item_func=IslamabadRSSPipeline.process_item,
                        max_workers=IslamabadRSSPipeline.MAX_WORKERS,
                        is_google_news=False,
                        apply_islamabad_filter=True,
                    )
                )
                time.sleep(10)

            logger.info("── Islamabad, Pakistan — Embassy  Feeds ──")
            for feed_url in EMBASSY_NEWS_FEEDS:
                all_articles.extend(
                    fetch_rss_feed(
                        feed_url=feed_url,
                        process_item_func=IslamabadRSSPipeline.process_item,
                        max_workers=IslamabadRSSPipeline.MAX_WORKERS,
                        is_google_news=True,
                        apply_islamabad_filter=False,
                        is_embassy_feed=True,
                    )
                )
                time.sleep(10)

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            all_articles = list({a["id"]: a for a in all_articles}.values())

            logger.info(f"After dedupe: {len(all_articles)} total articles")

            SupabaseClient.delete_old_articles(table_name=target_table)
            logger.info(
                f"Deleted articles older than 7 days from Supabase table '{target_table}'"
            )

            return SupabaseClient.insert_system_articles(
                "wafaq", all_articles, table_name=target_table
            )

        except Exception as e:
            logger.error(f"Islamabad Pulse pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }