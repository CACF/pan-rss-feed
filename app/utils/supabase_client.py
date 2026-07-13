import os
import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

# Meramurree database
SUPABASE_MERAMURREE_URL = os.getenv("SUPABASE_MERAMURREE_URL")
SUPABASE_MERAMURREE_KEY = os.getenv("SUPABASE_MERAMURREE_KEY")

# Table names
MERAMURREE_TABLE_NAME = "news_test"

# Meramurree Supabase client
meramurree_supabase = create_client(
    SUPABASE_MERAMURREE_URL,
    SUPABASE_MERAMURREE_KEY,
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
        table_name=MERAMURREE_TABLE_NAME,
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
                "pubDate": (
                    a.get("articlePubDate").isoformat()
                    if a.get("articlePubDate")
                    else None
                ),
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
            client=meramurree_supabase,
        )

    @staticmethod
    def insert_articles_current_year(
        article_list,
        table_name=MERAMURREE_TABLE_NAME,
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
                "pubDate": (
                    a.get("articlePubDate").isoformat()
                    if a.get("articlePubDate")
                    else None
                ),
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
            client=meramurree_supabase,
        )
