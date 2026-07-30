import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client
import config

logger = logging.getLogger(__name__)


def _init_client(url: str, key: str, fallback=None):
    """Safely initialize a Supabase client if URL and KEY are configured."""
    if url and key:
        try:
            return create_client(url, key)
        except Exception as e:
            logger.warning(f"Failed to initialize Supabase client for URL {url}: {e}")
    return fallback


# Category Supabase Clients
medianest_supabase = _init_client(
    config.SUPABASE_MEDIANEST_URL, config.SUPABASE_MEDIANEST_KEY
)
fashion_supabase = _init_client(
    config.SUPABASE_FASHION_URL, config.SUPABASE_FASHION_KEY, fallback=medianest_supabase
)
sports_supabase = _init_client(
    config.SUPABASE_SPORTS_URL, config.SUPABASE_SPORTS_KEY, fallback=medianest_supabase
)
supabase = medianest_supabase
fashionhub_supabase = _init_client(
    config.SUPABASE_FASHIONHUB_URL, config.SUPABASE_FASHIONHUB_KEY, fallback=fashion_supabase
)
houstonpulse_supabase = _init_client(
    config.SUPABASE_HOUSTONPULSE_URL,
    config.SUPABASE_HOUSTONPULSE_KEY,
    fallback=medianest_supabase,
)
medianestdev_supabase = _init_client(
    config.SUPABASE_MEDIANESTDEV_URL,
    config.SUPABASE_MEDIANESTDEV_KEY,
    fallback=medianest_supabase,
)
meramurree_supabase = _init_client(
    config.SUPABASE_MERAMURREE_URL, config.SUPABASE_MERAMURREE_KEY, fallback=supabase
)
merapeshawar_supabase = _init_client(
    config.SUPABASE_MERAPESHAWAR_URL,
    config.SUPABASE_MERAPESHAWAR_KEY,
    fallback=supabase,
)
sportifyhub_supabase = _init_client(
    config.SUPABASE_SPORTIFYHUB_URL, config.SUPABASE_SPORTIFYHUB_KEY, fallback=supabase
)
stylepulse_supabase = _init_client(
    config.SUPABASE_STYLEPULSE_URL, config.SUPABASE_STYLEPULSE_KEY, fallback=supabase
)
wafaq_supabase = _init_client(
    config.SUPABASE_WAFAQ_URL, config.SUPABASE_WAFAQ_KEY, fallback=supabase
)

# System Name Mapping
SYSTEM_CLIENT_MAP = {
    "business": {
        "client": medianest_supabase or supabase,
        "table": config.BUSINESS_TABLE,
    },
    "fashion": {"client": fashion_supabase, "table": config.FASHION_TABLE},
    "fashionhub": {"client": fashionhub_supabase, "table": config.FASHIONHUB_TABLE},
    "houstonpulse": {
        "client": houstonpulse_supabase,
        "table": config.HOUSTONPULSE_TABLE,
    },
    "medianest": {
        "client": medianest_supabase or supabase,
        "table": config.MEDIANEST_TABLE,
    },
    "sports": {"client": sports_supabase or supabase, "table": config.SPORTS_TABLE},
    "medianestdev": {
        "client": medianestdev_supabase,
        "table": config.MEDIANESTDEV_TABLE,
    },
    "meramurree": {"client": meramurree_supabase, "table": config.MERAMURREE_TABLE},
    "meramuree": {"client": meramurree_supabase, "table": config.MERAMURREE_TABLE},
    "merapeshawar": {
        "client": merapeshawar_supabase,
        "table": config.MERAPESHAWAR_TABLE,
    },
    "sportifyhub": {"client": sportifyhub_supabase, "table": config.SPORTIFYHUB_TABLE},
    "stylepulse": {"client": stylepulse_supabase, "table": config.STYLEPULSE_TABLE},
    "wafaq": {"client": wafaq_supabase, "table": config.WAFAQ_TABLE},
}

MAIN_TABLE_NAME = config.BUSINESS_TABLE
FASHION_TABLE_NAME = config.FASHION_TABLE


class SupabaseClient:
    """Utility class responsible for inserting, upserting, and deleting articles in Supabase."""

    @staticmethod
    def _upsert_articles(cleaned: list, table_name: str, client, max_retries: int = 3):
        import time

        if not cleaned or not client:
            return {"inserted_count": 0, "total_articles": 0}

        batch_size = 50
        total_inserted = 0

        for i in range(0, len(cleaned), batch_size):
            batch = cleaned[i : i + batch_size]
            for attempt in range(max_retries):
                try:
                    client.table(table_name).upsert(batch, on_conflict="id").execute()
                    total_inserted += len(batch)
                    break
                except Exception as e:
                    err_str = str(e)
                    if "PGRST204" in err_str or "Could not find the" in err_str:
                        for item in batch:
                            item.pop("articlePubDate", None)
                        try:
                            client.table(table_name).upsert(
                                batch, on_conflict="id"
                            ).execute()
                            total_inserted += len(batch)
                            break
                        except Exception as retry_e:
                            e = retry_e
                    if attempt == max_retries - 1:
                        logger.error(
                            f"Supabase upsert failed after {max_retries} attempts: {e}"
                        )
                        raise e
                    time.sleep(0.5)

        return {"inserted_count": total_inserted, "total_articles": len(cleaned)}

    @staticmethod
    def insert_articles(
        article_list: list, table_name: str = MAIN_TABLE_NAME, client=None, category: str = None
    ):
        if not client:
            cat = (category or "").lower()
            tb_lower = str(table_name or "").lower()
            sample_genre = (
                article_list[0].get("genre")
                if article_list and isinstance(article_list[0], dict) and article_list[0].get("genre")
                else ""
            ).lower()

            sports_genres = {
                "sports", "hockey", "golf", "soccer", "football", "cricket",
                "basketball", "baseball", "tennis", "mma", "ufc", "competition",
                "player", "quiz", "motorsport", "athletics", "boxing", "squash"
            }

            if cat == "sports" or "sports" in tb_lower or "sportify" in tb_lower or sample_genre in sports_genres:
                target_client = sports_supabase or sportifyhub_supabase or supabase
            elif cat == "fashion" or "fashion" in tb_lower or sample_genre == "fashion":
                target_client = fashion_supabase or fashionhub_supabase or supabase
            else:
                target_client = medianest_supabase or supabase
        else:
            target_client = client

        if not article_list or not target_client:
            return {"inserted_count": 0, "total_articles": 0}

        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        batch_time = datetime.now(timezone.utc).isoformat()

        filtered = [
            a
            for a in article_list
            if a.get("articlePubDate") and a["articlePubDate"] >= seven_days_ago
        ]

        cleaned = []
        for a in filtered:
            tags_val = a.get("tags")
            if isinstance(tags_val, (list, tuple)):
                tags_list = [t for t in tags_val if t]
            elif isinstance(tags_val, str) and tags_val.strip():
                tags_list = [tags_val.strip()]
            else:
                tags_list = []

            cleaned.append(
                {
                    "id": a.get("id"),
                    "title": a.get("title"),
                    "content": a.get("content"),
                    "authors": a.get("authors"),
                    "tags": tags_list,
                    "image": a.get("image"),
                    "articlePubDate": (
                        a["articlePubDate"].isoformat()
                        if a.get("articlePubDate")
                        else None
                    ),
                    "created_at": batch_time,
                    "source": a.get("source"),
                    "genre": a.get("genre"),
                    "language": a.get("language"),
                    "media_origin": a.get("media_origin"),
                }
            )

        return SupabaseClient._upsert_articles(
            cleaned=cleaned, table_name=table_name, client=target_client
        )

    @staticmethod
    def insert_system_articles(
        system_name: str, article_list: list, table_name: str = None
    ):
        """Insert articles into a specific system database by system name."""
        sys_info = SYSTEM_CLIENT_MAP.get(system_name.lower())
        if not sys_info:
            logger.warning(
                f"Unknown system name: '{system_name}'. Defaulting to main database client."
            )
            sys_info = SYSTEM_CLIENT_MAP["main"]

        target_table = table_name or sys_info["table"]
        target_client = sys_info["client"]

        return SupabaseClient.insert_articles(
            article_list, table_name=target_table, client=target_client
        )

    @staticmethod
    def delete_old_articles(
        table_name: str = MAIN_TABLE_NAME, client=None, days: int = 7
    ):
        """Delete articles older than `days` days from specified table."""
        target_client = client or supabase
        if not target_client:
            return {"deleted_count": 0, "error": "Supabase client not configured"}

        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        try:
            response = (
                target_client.table(table_name)
                .delete()
                .lt("articlePubDate", cutoff)
                .execute()
            )
            deleted_count = len(response.data) if response.data else 0
            logger.info(
                f"Deleted {deleted_count} articles older than {days} days from table '{table_name}'"
            )
            return {"deleted_count": deleted_count, "cutoff": cutoff}
        except Exception as e:
            logger.error(
                f"Failed to delete old articles from table '{table_name}': {e}"
            )
            return {"deleted_count": 0, "error": str(e)}

    @staticmethod
    def insert_articles_current_year(
        article_list: list, table_name: str = FASHION_TABLE_NAME
    ):
        target_client = fashion_supabase or supabase
        if not article_list or not target_client:
            return {"inserted_count": 0, "total_articles": 0}

        now = datetime.now(timezone.utc)
        start_of_year = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        batch_time = now.isoformat()

        filtered = [
            a
            for a in article_list
            if a.get("articlePubDate") and a["articlePubDate"] >= start_of_year
        ]

        cleaned = []
        for a in filtered:
            tags_val = a.get("tags")
            if isinstance(tags_val, (list, tuple)):
                tags_list = [t for t in tags_val if t]
            elif isinstance(tags_val, str) and tags_val.strip():
                tags_list = [tags_val.strip()]
            else:
                tags_list = []

            cleaned.append(
                {
                    "id": a.get("id"),
                    "title": a.get("title"),
                    "content": a.get("content"),
                    "authors": a.get("authors"),
                    "tags": tags_list,
                    "image": a.get("image"),
                    "created_at": batch_time,
                    "articlePubDate": (
                        a["articlePubDate"].isoformat()
                        if a.get("articlePubDate")
                        else None
                    ),
                    "source": a.get("source"),
                    "genre": a.get("genre"),
                    "language": a.get("language"),
                    "media_origin": a.get("media_origin"),
                }
            )

        return SupabaseClient._upsert_articles(
            cleaned=cleaned, table_name=table_name, client=target_client
        )
