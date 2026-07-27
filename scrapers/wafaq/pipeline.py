import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import WAFAQ_TABLE
from pipeline_maps.wafaq_pipeline_map import SCRAPERS

logger = logging.getLogger(__name__)


class WafaqPipeline:
    CATEGORY = "Wafaq"

    @classmethod
    def run_pipeline(
        cls,
        input_data=None,
        table_name=None,
        on_scraper_start=None,
        on_scraper_finish=None,
        max_workers=10,
    ):
        target_table = table_name or WAFAQ_TABLE
        results = []
        scrapers_to_run = SCRAPERS

        if not scrapers_to_run:
            return results

        def _run_single_scraper(scraper_cls):
            scraper_name = getattr(scraper_cls, "SOURCE", scraper_cls.__name__)
            if on_scraper_start:
                on_scraper_start(cls.CATEGORY, scraper_name)

            start_time = time.time()
            try:
                res = scraper_cls.run_pipeline(
                    input_data=input_data, table_name=target_table
                )
                scraped = res.get("total_articles", 0)
                inserted = res.get("inserted_count", 0)
                status = "Success" if "error" not in res else "Failed"
                err = res.get("error")
            except Exception as e:
                status = "Failed"
                scraped = 0
                inserted = 0
                err = str(e)

            elapsed = round(time.time() - start_time, 2)
            item_result = {
                "category": cls.CATEGORY,
                "scraper": scraper_name,
                "status": status,
                "scraped": scraped,
                "inserted": inserted,
                "execution_time": elapsed,
                "error": err,
            }

            if on_scraper_finish:
                on_scraper_finish(item_result)

            return item_result

        with ThreadPoolExecutor(
            max_workers=min(max_workers, len(scrapers_to_run))
        ) as executor:
            future_map = {
                executor.submit(_run_single_scraper, s): s for s in scrapers_to_run
            }

            for future in as_completed(future_map):
                results.append(future.result())

        return results
