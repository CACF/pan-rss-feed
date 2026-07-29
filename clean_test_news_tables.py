"""
Standalone Cleanup Script for Test News Tables

Deletes ALL rows exclusively from 'test_news' and 'news_test' tables across
every configured category / microservice database instance in Supabase.

Safety guarantees:
- Targets ONLY 'test_news' and 'news_test' tables.
- Does NOT modify, touch, or delete production tables ('news', 'fashion_news', etc.).
- Deduplicates clients so fallback instances are not wiped repeatedly.
- Clearly logs success and error statuses for each database.
"""

import logging
from app.utils.supabase_client import (
    supabase,
    fashion_supabase,
    fashionhub_supabase,
    houstonpulse_supabase,
    medianest_supabase,
    medianestdev_supabase,
    meramurree_supabase,
    merapeshawar_supabase,
    sportifyhub_supabase,
    stylepulse_supabase,
    wafaq_supabase,
)

# Setup clean console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# List of all configured Supabase clients with human-readable names
TARGET_DATABASES = [
    ("Main / Business", supabase),
    ("Fashion", fashion_supabase),
    ("FashionHub", fashionhub_supabase),
    ("HoustonPulse", houstonpulse_supabase),
    ("MediaNest", medianest_supabase),
    ("MediaNestDev", medianestdev_supabase),
    ("MeraMurree", meramurree_supabase),
    ("MeraPeshawar", merapeshawar_supabase),
    ("SportifyHub", sportifyhub_supabase),
    ("StylePulse", stylepulse_supabase),
    ("Wafaq", wafaq_supabase),
]

# Target table names to clean safely
TARGET_TABLE_NAMES = ["test_news", "news_test"]


def clean_test_tables():
    print("=" * 80)
    print("          STANDALONE TEST NEWS TABLES CLEANUP SCRIPT           ")
    print("=" * 80)
    print(f"Target Tables Allowed : {TARGET_TABLE_NAMES}")
    print(f"Total Database Clients: {len(TARGET_DATABASES)}")
    print("-" * 80)

    # Deduplicate clients by URL if fallback clients map to main instance
    seen_urls = set()
    unique_dbs = []
    for db_name, client in TARGET_DATABASES:
        if not client:
            logger.warning(
                f"[{db_name}] Supabase client not initialized (missing credentials). Skipping."
            )
            continue
        client_url = getattr(client, "supabase_url", None)
        if client_url and client_url in seen_urls:
            logger.info(
                f"[{db_name}] Shares same URL as previously processed database ({client_url}). Skipping duplicate."
            )
            continue
        if client_url:
            seen_urls.add(client_url)
        unique_dbs.append((db_name, client))

    summary = []

    for db_name, client in unique_dbs:
        cleared_any = False
        for table_name in TARGET_TABLE_NAMES:
            try:
                # Execute deletion of all rows (neq id 0 matches all UUID/string IDs)
                response = (
                    client.table(table_name)
                    .delete()
                    .neq("id", "00000000-0000-0000-0000-000000000000")
                    .execute()
                )
                deleted_count = (
                    len(response.data)
                    if hasattr(response, "data") and response.data
                    else 0
                )
                logger.info(
                    f"[{db_name:15s}] Successfully cleared table '{table_name}' | Rows deleted: {deleted_count}"
                )
                summary.append(
                    {
                        "database": db_name,
                        "table": table_name,
                        "status": "SUCCESS",
                        "rows_deleted": deleted_count,
                    }
                )
                cleared_any = True
            except Exception as e:
                err_msg = str(e)
                # Table not found (404 / PGRST205) is expected if test table doesn't exist on this DB
                if (
                    "PGRST205" in err_msg
                    or "404" in err_msg
                    or "Could not find the table" in err_msg
                ):
                    logger.debug(
                        f"[{db_name:15s}] Table '{table_name}' does not exist on this database."
                    )
                else:
                    logger.warning(
                        f"[{db_name:15s}] Failed to clear '{table_name}': {err_msg}"
                    )
                    summary.append(
                        {
                            "database": db_name,
                            "table": table_name,
                            "status": "FAILED",
                            "error": err_msg,
                        }
                    )

        if not cleared_any:
            logger.info(
                f"[{db_name:15s}] No test tables ('test_news' / 'news_test') found or modified."
            )

    print("=" * 80)
    print("                      CLEANUP SUMMARY REPORT                        ")
    print("=" * 80)
    for item in summary:
        if item["status"] == "SUCCESS":
            print(
                f" [OK]   {item['database']:15s} | Table: {item['table']:12s} | Status: SUCCESS | Deleted Rows: {item['rows_deleted']}"
            )
        else:
            print(
                f" [FAIL] {item['database']:15s} | Table: {item['table']:12s} | Status: FAILED  | Error: {item.get('error')}"
            )
    print("=" * 80)
    print("Cleanup completed safely.")


if __name__ == "__main__":
    clean_test_tables()
