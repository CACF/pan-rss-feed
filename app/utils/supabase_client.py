import os
import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


class SupabaseClient:

    @staticmethod
    def insert_articles(article_list, table_name="news"):
        if not article_list:
            return {"inserted_count": 0, "total_articles": 0}

        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

        # keep only recent articles
        recent_articles = [
            a
            for a in article_list
            if a.get("articlePubDate") and a["articlePubDate"] >= seven_days_ago
        ]

        if not recent_articles:
            return {"inserted_count": 0, "total_articles": 0}

        cleaned = []

        for a in recent_articles:
            item = dict(a)

            # map fields to your Supabase schema
            row = {
                "id": item.get("id"),  # REQUIRED PRIMARY KEY
                "title": item.get("title"),
                "content": item.get("content"),
                "authors": item.get("authors"),
                "tags": item.get("tags"),
                "image": item.get("image"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source": item.get("source"),
                "genre": item.get("genre"),
                "language": item.get("language"),
                "media_origin": item.get("media_origin"),
            }

            cleaned.append(row)

        supabase.table(table_name).upsert(cleaned, on_conflict="id").execute()

        return {"inserted_count": len(cleaned), "total_articles": len(cleaned)}
