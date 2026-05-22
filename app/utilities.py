import json
import logging
import xmltodict
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import os
from functools import lru_cache
from fake_useragent import UserAgent
from app.extensions import get_supabase

logger = logging.getLogger(__name__)


class MongoDBClient:
    """Supabase-backed client maintaining backward-compatible interface."""

    @staticmethod
    def insert_articles_to_mongo(article_list, user_email=None, collection_name="News"):
        if not article_list:
            return {"inserted_count": 0, "total_articles": 0}

        supabase = get_supabase()
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

        # Delete articles older than 7 days
        try:
            supabase.rpc("delete_old_news", {}).execute()
        except Exception as e:
            logger.warning(f"Failed to delete old news: {e}")

        recent_articles = [
            article for article in article_list
            if article.get("articlePubDate") and article["articlePubDate"] >= seven_days_ago
        ]

        if not recent_articles:
            logger.info("No articles within the last 7 days to insert.")
            return {"inserted_count": 0, "total_articles": 0}

        rows = []
        seen_ids = set()
        for article in recent_articles:
            article_id = article.get("_id")
            if not article_id or article_id in seen_ids:
                continue
            seen_ids.add(article_id)

            # Ensure tags is always a list, never a string or None
            raw_tags = article.get("tags", [])
            if isinstance(raw_tags, str):
                tags = [t.strip() for t in raw_tags.split(",") if t.strip()] if raw_tags.strip() else []
            elif isinstance(raw_tags, list):
                tags = raw_tags
            else:
                tags = []

            rows.append({
                "id": article_id,
                "title": article.get("title"),
                "content": article.get("content"),
                "authors": article.get("authors"),
                "tags": tags,
                "image": article.get("image"),
                "created_at": (
                    article["articlePubDate"].isoformat()
                    if article.get("articlePubDate") else None
                ),
                "source": article.get("source"),
                "genre": article.get("genre"),
                "language": article.get("language", "en-us"),
                "media_origin": article.get("media_origin"),
            })

        try:
            result = supabase.table("news").upsert(
                rows, on_conflict="id"
            ).execute()
            inserted_count = len(result.data) if result.data else 0
        except Exception as e:
            logger.error(f"Failed to upsert articles to Supabase: {e}")
            inserted_count = 0

        return {
            "inserted_count": inserted_count,
            "total_articles": len(recent_articles),
        }




class FeedParser:
    """A class to parse XML feed data"""

    @staticmethod
    def xml_to_json(media_origin, source, genre, xml_data):
        """A method to convert XML data to JSON"""

        parsed_list = []
        # Parse XML to Python dictionary
        parsed_data = xmltodict.parse(xml_data)

        # Convert to JSON
        json_data = json.dumps(parsed_data, indent=2, ensure_ascii=False)

        # Parse the JSON data to remove HTML tags from the 'description' field
        parsed_json = json.loads(json_data)

        language = (
            parsed_json.get("rss", "unknown")
            .get("channel", "unknown")
            .get("language", "unknown")
        )

        for item in parsed_json.get("rss").get("channel").get("item"):
            # Remove HTML tags from the 'description' field
            if "description" in item and media_origin == "foreign":
                soup = BeautifulSoup(item.get("description"), "html.parser")
                item["description"] = soup.get_text()
            elif "content:encoded" in item and media_origin == "local":
                soup = BeautifulSoup(item.get("content:encoded"), "html.parser")
                item["content:encoded"] = soup.get_text()

            parsed_list.append(item)

        # extracting feed build date to be used with every article in mongo document
        feedBuildDate = parsed_json.get("rss").get("channel").get("lastBuildDate")

        mongo_docs_list = FeedParser.prepare_news_documents(
            media_origin, source, genre, language, feedBuildDate, parsed_list
        )

        return mongo_docs_list

    @staticmethod
    def prepare_news_documents(
        media_origin, source, genre, language, feedBuildDate, parsed_list
    ):
        """Method to prepare the news documents for mongoDB"""
        document_list = []

        parsed_build_date = (
            datetime.strptime(feedBuildDate, "%a, %d %b %Y %H:%M:%S GMT")
            if media_origin == "foreign"
            else datetime.strptime(feedBuildDate, "%a, %d %b %y %H:%M:%S %z")
        )

        feed_build_date_tuple = FeedParser.create_date_tuple(parsed_build_date)

        for item in parsed_list:
            articlePubDate = item.get("pubDate")
            if articlePubDate:
                try:
                    parsed_pubDate = (
                        datetime.strptime(articlePubDate, "%a, %d %b %Y %H:%M:%S GMT")
                        if media_origin == "foreign"
                        else datetime.strptime(
                            articlePubDate, "%a, %d %b %y %H:%M:%S %z"
                        )
                    )

                    pub_date_tuple = FeedParser.create_date_tuple(parsed_pubDate)
                except Exception:
                    parsed_pubDate = None

            document = {}
            document["_id"] = item.get("link")
            document["media_origin"] = media_origin
            document["source"] = source
            document["genre"] = genre
            document["language"] = language
            document["feedBuildDate"] = datetime(*feed_build_date_tuple)
            document["title"] = item.get("title")
            document["content"] = (
                item.get("description")
                if media_origin == "foreign"
                else item.get("content:encoded")
            )
            document["articlePubDate"] = (
                datetime(*pub_date_tuple) if articlePubDate else None
            )
            document["tags"] = item.get("category")
            document["article_id"] = item.get("guid").get("#text")
            document["authors"] = item.get("author", source)

            document_list.append(document)

        return document_list

    @staticmethod
    def create_date_tuple(parsed_date):
        # Extract the individual date and time components
        year = parsed_date.year
        month = parsed_date.month
        day = parsed_date.day
        hour = parsed_date.hour
        minute = parsed_date.minute
        second = parsed_date.second

        return (
            year,
            month,
            day,
            hour,
            minute,
            second,
        )





def convert_objectids(obj):
    if isinstance(obj, dict):
        return {k: convert_objectids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectids(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


# --- HTTP utilities ---
@lru_cache(maxsize=1)
def _get_user_agent_rotator():
    try:
        return UserAgent()
    except Exception:
        return None


def get_random_headers(base: dict | None = None) -> dict:
    """Return headers with a randomized User-Agent merged over base.

    Fallbacks to a static UA if fake_useragent fails.
    """
    headers = dict(base or {})
    rotator = _get_user_agent_rotator()
    ua_value = None
    try:
        if rotator is not None:
            ua_value = rotator.random
    except Exception:
        ua_value = None
    if not ua_value:
        ua_value = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    headers.setdefault("User-Agent", ua_value)
    headers.setdefault("Accept-Language", "en-US,en;q=0.9")
    return headers
