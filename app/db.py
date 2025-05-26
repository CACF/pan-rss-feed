import os
import asyncio
from urllib.parse import quote_plus
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import pymongo.errors

# Load env vars once at module load
load_dotenv()


class MongoDBAsyncClient:
    # Load environment variables as class attributes or inside __init__
    USERNAME = os.getenv("REUTERS_USERNAME")
    PASSWORD = os.getenv("REUTERS_PASSWORD")
    MONGO_USER = os.getenv("MONGO_USERNAME")
    MONGO_PASS = os.getenv("MONGO_PASSWORD")
    MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT = os.getenv("MONGO_PORT", "27017")
    MONGO_DB = os.getenv("DATABASE")
    MONGO_COLLECTION = os.getenv("COLLECTION", "News")

    def __init__(self):
        connection_string = f"mongodb://{self.MONGO_USER}:{self.MONGO_PASS}@{self.MONGO_HOST}:{self.MONGO_PORT}/"

        self.client = AsyncIOMotorClient(connection_string)
        self.db = self.client[self.MONGO_DB]

    async def insert_documents_with_retry(
        self, collection_name=None, document_list=None, max_retries=3
    ):
        if collection_name is None:
            collection_name = self.MONGO_COLLECTION
        if document_list is None or not isinstance(document_list, list):
            raise ValueError("document_list must be a non-empty list.")

        collection = self.db[collection_name]
        for i in range(0, len(document_list), 100):
            batch = document_list[i : i + 100]
            operations = [
                UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
                for doc in batch
            ]
            for attempt in range(max_retries):
                try:
                    await collection.bulk_write(operations)
                    break
                except pymongo.errors.AutoReconnect as e:
                    print(
                        f"AutoReconnect error: {e}, retrying ({attempt+1}/{max_retries})..."
                    )
                    await asyncio.sleep(2**attempt)
                except Exception as e:
                    print(f"Unexpected error: {e}")
                    break
