import time
import logging
import threading
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_registry import PIPELINES


class LogBufferHandler(logging.Handler):
    """Custom logging handler to buffer warnings/errors and group them by scraper module."""

    def __init__(self):
        super().__init__()
        self.grouped_records = collections.defaultdict(list)

    def emit(self, record):
        try:
            module = record.name
            clean_name = module.split(".")[-1] if "." in module else module
            msg = record.getMessage()
            formatted_item = f"{msg}"
            self.grouped_records[clean_name].append(formatted_item)
        except Exception:
            pass


log_buffer = LogBufferHandler()
log_buffer.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Configure root logger to route WARNING and above to log_buffer during execution
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARNING)
# Remove default handlers to prevent real-time stream cluttering stdout/stderr
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
root_logger.addHandler(log_buffer)

logger = logging.getLogger(__name__)

# Lock to guarantee clean, non-interleaved terminal output across threads
console_lock = threading.Lock()


def summarize_error(err_str: str) -> str:
    """Convert raw exception strings into concise, human-readable summary failure reasons."""
    if not err_str:
        return "Unknown error"
    err_lower = str(err_str).lower()

    if any(k in err_lower for k in ["timeout", "timed out", "readtimeouterror"]):
        return "Request timeout"
    if any(k in err_lower for k in ["disconnected", "connection error", "connection reset", "remotedisconnected"]):
        return "Server connection reset"
    if any(k in err_lower for k in ["table not found", "relation", "does not exist", "table"]):
        return "Database table not found"
    if any(k in err_lower for k in ["403", "forbidden", "unauthorized", "auth", "permission"]):
        return "Authentication / Access forbidden"
    if "no articles" in err_lower:
        return "No articles found"
    if any(k in err_lower for k in ["array", "malformed", "schema", "typeerror", "22p02"]):
        return "Data schema parsing error"
    if any(k in err_lower for k in ["multiple values for argument", "nameerror", "attributeerror"]):
        return "Scraper code execution error"

    clean_err = str(err_str).split("\n")[0].strip()
    return clean_err[:60] + ("..." if len(clean_err) > 60 else "")


def log_scraper_start(category: str, scraper_name: str):
    """Callback triggered when a scraper starts execution."""
    with console_lock:
        print(f"[{category}] Running: {scraper_name}...")


def log_scraper_finish(result: dict):
    """Callback triggered when a scraper finishes execution."""
    category = result["category"]
    scraper = result["scraper"]
    status = result["status"]
    scraped = result["scraped"]
    inserted = result["inserted"]
    exec_time = result["execution_time"]
    err = result.get("error")

    status_tag = "SUCCESS" if status == "Success" else "FAILED"

    with console_lock:
        output_line = (
            f"[{category:<13}] {scraper:<32} | Status: {status_tag:<7} | "
            f"Scraped: {scraped:<4} | Inserted: {inserted:<4} | Time: {exec_time:.2f}s"
        )
        if err:
            output_line += f" | Error: {err}"
        print(output_line)


def run_pipeline_wrapper(pipeline_cls):
    """Execute a single category pipeline safely."""
    try:
        results = pipeline_cls.run_pipeline(
            on_scraper_start=log_scraper_start,
            on_scraper_finish=log_scraper_finish,
        )
        return results
    except Exception as e:
        logger.error(f"Failed to execute pipeline {pipeline_cls.__name__}: {e}")
        return []


def main():
    if not PIPELINES:
        print("No pipelines registered or enabled in pipeline_registry.py.")
        return

    print("=" * 80)
    print("                      PAN RSS FEED SCRAPER RUNNER                       ")
    print("=" * 80)
    print(f"Registered Category Pipelines: {[p.CATEGORY for p in PIPELINES]}")
    print("-" * 80)

    start_total_time = time.time()
    all_results = []

    # Execute all enabled category pipelines concurrently
    with ThreadPoolExecutor(max_workers=len(PIPELINES)) as executor:
        future_map = {executor.submit(run_pipeline_wrapper, p): p for p in PIPELINES}

        for future in as_completed(future_map):
            pipeline_cls = future_map[future]
            try:
                pipeline_results = future.result()
                all_results.extend(pipeline_results)
            except Exception as e:
                logger.error(f"Error reading result from {pipeline_cls.__name__}: {e}")

    total_execution_time = round(time.time() - start_total_time, 2)

    # Compute Summary Statistics
    total_scrapers = len(all_results)
    successful_count = sum(1 for r in all_results if r["status"] == "Success")
    failed_count = sum(1 for r in all_results if r["status"] == "Failed")
    total_scraped_articles = sum(r["scraped"] for r in all_results)
    total_rows_inserted = sum(r["inserted"] for r in all_results)

    # Group results by Category
    category_results = collections.defaultdict(list)
    for r in all_results:
        cat = r.get("category", "Unknown")
        category_results[cat].append(r)

    print("\n" + "=" * 80)
    print("                           CATEGORY EXECUTION SUMMARIES                        ")
    print("=" * 80)

    for cat_name, cat_items in category_results.items():
        cat_total_scrapers = len(cat_items)
        cat_success = sum(1 for r in cat_items if r["status"] == "Success")
        cat_failed = sum(1 for r in cat_items if r["status"] == "Failed")
        cat_scraped = sum(r["scraped"] for r in cat_items)
        cat_inserted = sum(r["inserted"] for r in cat_items)

        header_str = f" {cat_name.upper()} "
        print(f"\n{header_str:=^80}")
        print(f" Total Scrapers Run   : {cat_total_scrapers}")
        print(f" Successful Scrapers  : {cat_success}")
        print(f" Failed Scrapers      : {cat_failed}")
        print(f" Total Articles Found : {cat_scraped}")
        print(f" Total Rows Inserted  : {cat_inserted}")

        if cat_failed > 0:
            print("\n Failed Scrapers:")
            failed_in_cat = [r for r in cat_items if r["status"] == "Failed"]
            for f_item in failed_in_cat:
                scraper_title = f_item.get("scraper", "Unknown")
                raw_err = f_item.get("error", "")
                reason = summarize_error(raw_err)
                print(f"  - {scraper_title:<20}: {reason}")

    print("\n" + "=" * 80)
    print("                                OVERALL SUMMARY                         ")
    print("=" * 80)
    print(f" Total Scrapers Run   : {total_scrapers}")
    print(f" Successful Scrapers  : {successful_count}")
    print(f" Failed Scrapers      : {failed_count}")
    print(f" Total Articles Found : {total_scraped_articles}")
    print(f" Total Rows Inserted  : {total_rows_inserted}")
    print(f" Total Execution Time : {total_execution_time} seconds")
    print("=" * 80)

    # Display warnings and log messages grouped by scraper module
    if log_buffer.grouped_records:
        print("\n" + "-" * 80)
        print("                  WARNINGS & LOG MESSAGES BY SCRAPER                   ")
        print("-" * 80)
        for idx, (scraper_name, messages) in enumerate(log_buffer.grouped_records.items(), start=1):
            count = len(messages)
            plural = "s" if count > 1 else ""
            print(f" {idx}. [{scraper_name}] ({count} Warning{plural} / Issue{plural}):")
            for msg in messages:
                print(f"    • {msg}")

    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
