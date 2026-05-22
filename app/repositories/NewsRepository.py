from datetime import datetime
import logging
from app.extensions import get_supabase

logger = logging.getLogger(__name__)


class News:
    @staticmethod
    def create_indexes():
        """Indexes are managed via SQL migrations in Supabase."""
        pass

    @staticmethod
    def filter_documents(
        session=None,
        search: str = None,
        sources: list = None,
        genres: list = None,
        filter_datetime: datetime = None,
        maxArticles=None,
    ):
        supabase = get_supabase()

        params = {
            "p_search": search or None,
            "p_sources": sources if sources else None,
            "p_genres": genres if genres else None,
            "p_start_date": None,
            "p_end_date": None,
            "p_max_articles": None,
        }

        if filter_datetime:
            start_of_day = filter_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = filter_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
            params["p_start_date"] = start_of_day.isoformat()
            params["p_end_date"] = end_of_day.isoformat()
        try:
            max_articles = int(maxArticles) if maxArticles is not None else None
            if max_articles and max_articles > 0:
                params["p_max_articles"] = max_articles
        except (ValueError, TypeError):
            pass

        logger.debug(f"Supabase RPC params: {params}")
        result = supabase.rpc("get_filtered_news", params).execute()
        logger.debug(f"Result Count: {len(result.data)}")
        return result.data

    @staticmethod
    def get_appnews_genres(session=None):
        supabase = get_supabase()
        result = (
            supabase.table("news")
            .select("genre")
            .eq("source", "APP NEWS")
            .execute()
        )
        genres = {doc["genre"].strip() for doc in result.data if doc.get("genre")}
        genres.add("General")
        return sorted(genres)

    @staticmethod
    def get_distinct_sources():
        supabase = get_supabase()
        result = supabase.rpc("get_distinct_sources", {}).execute()
        return [row["source"] for row in result.data if row.get("source")]

    @staticmethod
    def get_distinct_genres_for_source(source: str):
        supabase = get_supabase()
        result = supabase.rpc(
            "get_distinct_genres_for_source", {"p_source": source}
        ).execute()
        return [row["genre"] for row in result.data if row.get("genre")]
