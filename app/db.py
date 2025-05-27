import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import pymongo.errors
from config import settings


class MongoDBAsyncClient:
    def __init__(self):

        self.mongo_user = settings.MONGO_USERNAME
        self.mongo_pass = settings.MONGO_PASSWORD
        self.mongo_host = settings.MONGO_HOST or "localhost"
        self.mongo_port = settings.MONGO_PORT or "27017"
        self.mongo_db = settings.DATABASE
        self.mongo_collection = settings.COLLECTION

        connection_string = f"mongodb://{self.mongo_user}:{self.mongo_pass}@{self.mongo_host}:{self.mongo_port}/"

        self.client = AsyncIOMotorClient(connection_string)
        self.db = self.client[self.mongo_db]

    async def insert_documents_with_retry(
        self, collection_name=None, document_list=None, max_retries=5
    ):
        if collection_name is None:
            collection_name = self.mongo_collection
        if document_list is None or not isinstance(document_list, list):
            raise ValueError("document_list must be a non-empty list.")

        collection = self.db[collection_name]

        total_inserts = 0

        for document in range(0, len(document_list), 100):
            batch = document_list[document : document + 100]
            operations = [
                UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
                for doc in batch
            ]
            for attempt in range(max_retries):
                try:
                    result = await collection.bulk_write(operations)
                    total_inserts += result.upserted_count
                    break
                except pymongo.errors.AutoReconnect as e:
                    print(
                        f"AutoReconnect error: {e}, retrying ({attempt+1}/{max_retries})..."
                    )
                    await asyncio.sleep(2**attempt)
                except Exception as e:
                    print(f"Unexpected error: {e}")
                    break

        return total_inserts
