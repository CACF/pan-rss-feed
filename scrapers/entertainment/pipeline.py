import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import ENTERTAINMENT_TABLE
from pipeline_maps.entertainment_pipeline_map import SCRAPERS

logger = logging.getLogger(__name__)


class EntertainmentPipeline:
    """Pipeline for managing and running all Entertainment scrapers from entertainment_pipeline_map.py."""

    CATEGORY = "Entertainment"

    @classmethod
    def run_pipeline(cls, input_data=None, table_name=None, on_scraper_start=None, on_scraper_finish=None, max_workers=10):
        """Execute all entertainment scrapers registered in entertainment_pipeline_map.py concurrently."""
        target_table = table_name or ENTERTAINMENT_TABLE
        results = []
        scrapers_to_run = SCRAPERS

        if not scrapers_to_run:
            logger.info("No entertainment scrapers enabled in pipeline_maps/entertainment_pipeline_map.py")
            return results

        def _run_single_scraper(scraper_cls):
            scraper_name = getattr(scraper_cls, "SOURCE", scraper_cls.__name__)

            if on_scraper_start:
                on_scraper_start(cls.CATEGORY, scraper_name)

            start_time = time.time()
            status = "Success"
            articles_found = 0
            inserted_count = 0
            error_msg = None

            try:
                try:
                    res = scraper_cls.run_pipeline(input_data=input_data, table_name=target_table)
                except TypeError:
                    res = scraper_cls.run_pipeline(input_data=input_data)

                if isinstance(res, dict):
                    articles_found = res.get("total_articles", 0)
                    inserted_count = res.get("inserted_count", 0)
                    if "error" in res:
                        status = "Failed"
                        error_msg = res["error"]
            except Exception as e:
                status = "Failed"
                error_msg = str(e)
                logger.error(f"Error running scraper {scraper_name}: {e}")

            elapsed = round(time.time() - start_time, 2)
            item_result = {
                "category": cls.CATEGORY,
                "scraper": scraper_name,
                "status": status,
                "scraped": articles_found,
                "inserted": inserted_count,
                "execution_time": elapsed,
                "error": error_msg,
            }

            if on_scraper_finish:
                on_scraper_finish(item_result)

            return item_result

        workers = min(max_workers, len(scrapers_to_run)) or 1
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(_run_single_scraper, scraper_cls): scraper_cls for scraper_cls in scrapers_to_run}
            for future in as_completed(future_map):
                try:
                    item_result = future.result()
                    results.append(item_result)
                except Exception as e:
                    logger.error(f"Error retrieving scraper result in {cls.CATEGORY}: {e}")

        return results
