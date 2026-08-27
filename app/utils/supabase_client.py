import time
import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client
import config

logger = logging.getLogger(__name__)


def create_supabase_client(url: str, key: str):
    """Safely initialize a Supabase client if URL and KEY are configured."""
    if url and key:
        try:
            return create_client(url, key)
        except Exception as e:
            logger.warning(f"Failed to initialize Supabase client for URL {url}: {e}")
    return None


def _clean_str(val):
    """Strip NUL bytes from strings to prevent PostgreSQL 22P05 Unicode errors."""
    if isinstance(val, str):
        return val.replace("\x00", "").replace("\u0000", "")
    return val


# System Supabase Clients (Direct instantiation without fallback database mixing)
business_supabase = create_supabase_client(
    config.SUPABASE_MEDIANEST_URL, config.SUPABASE_MEDIANEST_KEY
)
medianest_supabase = business_supabase
supabase = business_supabase

sports_supabase = create_supabase_client(
    config.SUPABASE_SPORTS_URL, config.SUPABASE_SPORTS_KEY
)

fashion_supabase = create_supabase_client(
    config.SUPABASE_FASHION_URL, config.SUPABASE_FASHION_KEY
)

houstonpulse_supabase = create_supabase_client(
    config.SUPABASE_HOUSTONPULSE_URL, config.SUPABASE_HOUSTONPULSE_KEY
)

meramurree_supabase = create_supabase_client(
    config.SUPABASE_MERAMURREE_URL, config.SUPABASE_MERAMURREE_KEY
)

wafaq_supabase = create_supabase_client(
    config.SUPABASE_WAFAQ_URL, config.SUPABASE_WAFAQ_KEY
)

merapeshawar_supabase = create_supabase_client(
    config.SUPABASE_MERAPESHAWAR_URL, config.SUPABASE_MERAPESHAWAR_KEY
)

karachi_supabase = create_supabase_client(
    config.SUPABASE_KARACHI_URL, config.SUPABASE_KARACHI_KEY
)

# Explicit System mapping for clients and tables
SYSTEM_CLIENTS = {
    "business": {"client": business_supabase, "table": config.BUSINESS_TABLE},
    "medianest": {"client": business_supabase, "table": config.BUSINESS_TABLE},
    "sports": {"client": sports_supabase, "table": config.SPORTS_TABLE},
    "fashion": {"client": fashion_supabase, "table": config.FASHION_TABLE},
    "houstonpulse": {
        "client": houstonpulse_supabase,
        "table": config.HOUSTONPULSE_TABLE,
    },
    "meramurree": {"client": meramurree_supabase, "table": config.MERAMURREE_TABLE},
    "meramuree": {"client": meramurree_supabase, "table": config.MERAMURREE_TABLE},
    "wafaq": {"client": wafaq_supabase, "table": config.WAFAQ_TABLE},
    "entertainment": {"client": business_supabase, "table": config.ENTERTAINMENT_TABLE},
    "merapeshawar": {
        "client": merapeshawar_supabase,
        "table": config.MERAPESHAWAR_TABLE,
    },
    "karachi": {"client": karachi_supabase, "table": config.KARACHI_TABLE},
}
SYSTEM_CLIENT_MAP = SYSTEM_CLIENTS  # Alias for backward compatibility if needed


class SupabaseClient:
    """Utility class responsible for inserting, upserting, and deleting articles in Supabase."""

    @staticmethod
    def _upsert_articles(cleaned: list, table_name: str, client, max_retries: int = 3):
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
        article_list: list, table_name: str, client=None, category: str = None
    ):
        """Insert articles into a specified table using an explicitly provided client or system category."""
        target_client = client
        if not target_client:
            cat = (category or "business").lower()
            sys_info = SYSTEM_CLIENTS.get(cat)
            if sys_info:
                target_client = sys_info["client"]

        if not target_client or not table_name:
            logger.error(
                f"Missing client or table_name for insert_articles. table_name='{table_name}', client={target_client}"
            )
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": f"Missing client or table_name (table_name='{table_name}', client={target_client})",
            }

        if not article_list:
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
                tags_list = [_clean_str(t) for t in tags_val if t]
            elif isinstance(tags_val, str) and tags_val.strip():
                tags_list = [_clean_str(tags_val.strip())]
            else:
                tags_list = []

            cleaned.append(
                {
                    "id": _clean_str(a.get("id")),
                    "title": _clean_str(a.get("title")),
                    "content": _clean_str(a.get("content")),
                    "authors": _clean_str(a.get("authors")),
                    "tags": tags_list,
                    "image": _clean_str(a.get("image")),
                    "articlePubDate": (
                        a["articlePubDate"].isoformat()
                        if a.get("articlePubDate")
                        else None
                    ),
                    "created_at": batch_time,
                    "source": _clean_str(a.get("source")),
                    "genre": _clean_str(a.get("genre")),
                    "language": _clean_str(a.get("language")),
                    "media_origin": _clean_str(a.get("media_origin")),
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
        sys_info = SYSTEM_CLIENTS.get(system_name.lower())
        if not sys_info:
            logger.error(f"Unknown system name: '{system_name}'")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": f"Unknown system name: '{system_name}'",
            }

        target_table = table_name or sys_info["table"]
        target_client = sys_info["client"]

        return SupabaseClient.insert_articles(
            article_list, table_name=target_table, client=target_client
        )

    @staticmethod
    def insert_articles_current_year(
        article_list: list, table_name: str, client=None, category: str = None
    ):
        """Insert articles published in the current year."""
        target_client = client
        if not target_client:
            cat = (category or "fashion").lower()
            sys_info = SYSTEM_CLIENTS.get(cat)
            if sys_info:
                target_client = sys_info["client"]

        if not target_client or not table_name:
            logger.error(
                f"Missing client or table_name for insert_articles_current_year. table_name='{table_name}', client={target_client}"
            )
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": f"Missing client or table_name (table_name='{table_name}', client={target_client})",
            }

        if not article_list:
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
                tags_list = [_clean_str(t) for t in tags_val if t]
            elif isinstance(tags_val, str) and tags_val.strip():
                tags_list = [_clean_str(tags_val.strip())]
            else:
                tags_list = []

            cleaned.append(
                {
                    "id": _clean_str(a.get("id")),
                    "title": _clean_str(a.get("title")),
                    "content": _clean_str(a.get("content")),
                    "authors": _clean_str(a.get("authors")),
                    "tags": tags_list,
                    "image": _clean_str(a.get("image")),
                    "created_at": batch_time,
                    "articlePubDate": (
                        a["articlePubDate"].isoformat()
                        if a.get("articlePubDate")
                        else None
                    ),
                    "source": _clean_str(a.get("source")),
                    "genre": _clean_str(a.get("genre")),
                    "language": _clean_str(a.get("language")),
                    "media_origin": _clean_str(a.get("media_origin")),
                }
            )

        return SupabaseClient._upsert_articles(
            cleaned=cleaned, table_name=table_name, client=target_client
        )

    @staticmethod
    def delete_old_articles(
        table_name: str, client=None, category: str = None, days: int = 7
    ):
        """Delete articles older than `days` days from specified table."""
        target_client = client
        if not target_client:
            cat = (category or "business").lower()
            sys_info = SYSTEM_CLIENTS.get(cat)
            if sys_info:
                target_client = sys_info["client"]

        if not target_client or not table_name:
            return {
                "deleted_count": 0,
                "error": "Supabase client or table_name not specified",
            }

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
