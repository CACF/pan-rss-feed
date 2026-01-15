import json
import logging
import xmltodict
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from bson import ObjectId
import os
from functools import lru_cache
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)

class MongoDBClient:
    def __init__(self, connection_string, db_name):
        self.client = MongoClient(connection_string)
        self.db = self.client[db_name]

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def insert_documents(self, collection_name, document_list):
        collection = self.db[collection_name]
        inserted_ids = []
        for document in document_list:
            result = collection.update_one(
                {"_id": document["_id"]}, {"$set": document}, upsert=True
            )
            if result.upserted_id or result.modified_count:
                inserted_ids.append(document["_id"])
        return inserted_ids



    @staticmethod
    def insert_articles_to_mongo(article_list, user_email=None, collection_name="News"):
        if not article_list:
            return {"inserted_count": 0, "total_articles": 0}

        connection_string = (
            f"mongodb://{os.getenv('DB_USER')}:{os.getenv('DB_PW')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        )
        db_name = os.getenv("DB_NAME", "Karobaar")

        with MongoDBClient(connection_string, db_name) as mongo_client:
            collection = mongo_client.db[collection_name]
            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

            delete_result = collection.delete_many({
                "articlePubDate": {"$lt": seven_days_ago}
            })

            logger.info(
                f"Deleted {delete_result.deleted_count} old articles from {collection_name}"
            )
            inserted_ids = mongo_client.insert_documents(collection_name, article_list)

        return {
            "inserted_count": len(inserted_ids),
            "total_articles": len(article_list)
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
