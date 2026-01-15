from datetime import datetime
import os
from app.extensions import mongo
from pymongo.client_session import ClientSession
import re


class News:
    @staticmethod
    def escape_regex_if_special(text):
        text = text.strip()
        if re.match(r"^\W+$", text):
            return re.escape(text)
        return text

    """ Checking and creating indexes """

    @staticmethod
    def create_indexes():
        collection_name = os.getenv("NEWS_COLLECTION", "News")
        db_name = os.getenv("DB_NAME", "Karobaar")
        collection = mongo.db[collection_name]

        existing_indexes = collection.index_information()

        if "title_text_index" not in existing_indexes:
            collection.create_index(
                [("title", "text")],
                default_language="english",
                language_override="english",
            )

        if "source_genre_datetime_index" not in existing_indexes:
            collection.create_index(
                [("source", 1), ("genre", 1), ("articlePubDate", -1)]
            )

        if "scraped_at_index" not in existing_indexes:
            collection.create_index([("scraped_at", -1)])

        if "source_index" not in existing_indexes:
            collection.create_index([("source", 1)])

        if "article_id_index" not in existing_indexes:
            collection.create_index([("article_id", 1)], unique=True)

    """ Filter Documents """

    @staticmethod
    def filter_documents(
        session: ClientSession,
        search: str = None,
        sources: list = None,
        genres: list = None,
        filter_datetime: datetime = None,
        maxArticles=None,
    ):
        mongo_query = {}

        if search:
            search_query = News.escape_regex_if_special(search)
            mongo_query["$or"] = [
                {"title": {"$regex": search_query, "$options": "i"}},
                {"content": {"$regex": search_query, "$options": "i"}},
                {"tags": {"$regex": search_query, "$options": "i"}},
            ]

        if sources:
            mongo_query["source"] = {"$in": sources}

        if genres:
            mongo_query["genre"] = {
                "$in": [re.compile(f"^{re.escape(g)}$", re.IGNORECASE) for g in genres]
            }
        if filter_datetime:
            start_of_day = filter_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = filter_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
            mongo_query["articlePubDate"] = {"$gte": start_of_day, "$lte": end_of_day}

        pipeline = [
            {"$match": mongo_query},
            {"$sort": {"articlePubDate": -1}},
            {
                "$project": {
                    "_id": 1,
                    "article_id": 1,
                    "title": 1,
                    "articlePubDate": 1,
                    "source": 1,
                    "genre": 1,
                    "media_origin": 1,
                    "feedBuildDate": 1,
                    "tags": 1,
                    "authors": 1,
                    "content": 1,
                    "language": 1,
                }
            },
        ]

        try:
            if maxArticles is not None:
                maxArticles = int(maxArticles)
                if maxArticles > 0:
                    pipeline.append({"$limit": maxArticles})
        except (ValueError, TypeError):
            pass

        collection_name = "News"
        result = list(mongo.db[collection_name].aggregate(pipeline))

        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Final Mongo Query: {mongo_query}")
        logger.debug(f"Pipeline: {pipeline}")
        logger.debug(f"Result Count: {len(result)}")
        return result

    
    @staticmethod
    def get_appnews_genres(session):
        genres = set()
        collection_name = os.getenv("NEWS_COLLECTION", "News")
        cursor = mongo.db[collection_name].find({"source": "APP NEWS"}, {"genre": 1})
        for doc in cursor:
            genre = doc.get("genre")
            if genre:
                genres.add(genre.strip())
        genres.add("General")
        return sorted(genres)
