import os
import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

# Main database (Business, Sports, etc.)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Fashion database
SUPABASE_FASHION_URL = os.getenv("SUPABASE_FASHION_URL")
SUPABASE_FASHION_KEY = os.getenv("SUPABASE_FASHION_KEY")

# Table names
MAIN_TABLE_NAME = "news"
FASHION_TABLE_NAME = "news"

# Main Supabase client
supabase = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
)

# Fashion Supabase client
fashion_supabase = create_client(
    SUPABASE_FASHION_URL,
    SUPABASE_FASHION_KEY,
)


class SupabaseClient:

    @staticmethod
    def _upsert_articles(
        cleaned,
        table_name,
        client,
    ):

        if not cleaned:
            return {
                "inserted_count": 0,
                "total_articles": 0,
            }

        client.table(table_name).upsert(
            cleaned,
            on_conflict="id",
        ).execute()

        return {
            "inserted_count": len(cleaned),
            "total_articles": len(cleaned),
        }

    @staticmethod
    def insert_articles(
        article_list,
        table_name=MAIN_TABLE_NAME,
    ):

        if not article_list:
            return {
                "inserted_count": 0,
                "total_articles": 0,
            }

        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

        batch_time = datetime.now(timezone.utc).isoformat()

        filtered = [
            a
            for a in article_list
            if a.get("articlePubDate") and a["articlePubDate"] >= seven_days_ago
        ]

        cleaned = [
            {
                "id": a.get("id"),
                "title": a.get("title"),
                "content": a.get("content"),
                "authors": a.get("authors"),
                "tags": a.get("tags"),
                "image": a.get("image"),
                "created_at": batch_time,
                "source": a.get("source"),
                "genre": a.get("genre"),
                "language": a.get("language"),
                "media_origin": a.get("media_origin"),
            }
            for a in filtered
        ]

        return SupabaseClient._upsert_articles(
            cleaned=cleaned,
            table_name=table_name,
            client=supabase,
        )

    @staticmethod
    def insert_articles_current_year(
        article_list,
        table_name=FASHION_TABLE_NAME,
    ):

        if not article_list:
            return {
                "inserted_count": 0,
                "total_articles": 0,
            }

        now = datetime.now(timezone.utc)

        start_of_year = datetime(
            now.year,
            1,
            1,
            tzinfo=timezone.utc,
        )

        batch_time = now.isoformat()

        filtered = [
            a
            for a in article_list
            if a.get("articlePubDate") and a["articlePubDate"] >= start_of_year
        ]

        cleaned = [
            {
                "id": a.get("id"),
                "title": a.get("title"),
                "content": a.get("content"),
                "authors": a.get("authors"),
                "tags": a.get("tags"),
                "image": a.get("image"),
                "created_at": batch_time,
                "source": a.get("source"),
                "genre": a.get("genre"),
                "language": a.get("language"),
                "media_origin": a.get("media_origin"),
            }
            for a in filtered
        ]

        return SupabaseClient._upsert_articles(
            cleaned=cleaned,
            table_name=table_name,
            client=fashion_supabase,
        )
