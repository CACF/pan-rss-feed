import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from app.news_sources.pipeline_map import PIPELINE_MAP

logger = logging.getLogger(__name__)

# Import celery from app context
try:
    from app import celery
except ImportError:
    # Fallback for when celery is not yet initialized
    celery = None


def run_news_scrapers_task(sources=None, interval_minutes=30, user_id=None):
    """
    Celery task to run news scrapers for specified sources.

    Args:
        sources (list): List of source names to scrape (default: all)
        interval_minutes (int): Interval in minutes for scheduling (default: 30)
        user_id (str): Optional user ID for tracking

    Returns:
        dict: Results summary
    """
    try:
        start_time = datetime.now(timezone.utc)
        logger.info(f"Starting news scraping task at {start_time}")
        logger.info(f"Sources: {sources}, Interval: {interval_minutes} minutes")

        # If no sources specified, use all available

        if not sources:
            sources = list(PIPELINE_MAP.keys())

        results = {
            "task_started_at": start_time.isoformat(),
            "sources": sources,
            "interval_minutes": interval_minutes,
            "results": {},
            "total_articles": 0,
            "total_inserted": 0,
        }

        # Run each source pipeline
        for source in sources:

            if source not in PIPELINE_MAP:
                logger.warning(f"Unknown source: {source}")
                results["results"][source] = {
                    "status": "error",
                    "message": f"Unknown source: {source}",
                    "articles_found": 0,
                    "articles_inserted": 0,
                }
                continue

            try:
                logger.info(f"Running pipeline for source: {source}")
                pipeline_class = PIPELINE_MAP[source]

                # Prepare input data
                input_data = {"email": user_id} if user_id else {}

                # Run the pipeline
                pipeline_result = pipeline_class.run_pipeline(input_data)

                # Extract results
                articles_found = pipeline_result.get("total_articles", 0)
                articles_inserted = pipeline_result.get("inserted_count", 0)

                results["results"][source] = {
                    "status": "success",
                    "articles_found": articles_found,
                    "articles_inserted": articles_inserted,
                    "elapsed_time": pipeline_result.get("elapsed_time", 0),
                }

                results["total_articles"] += articles_found
                results["total_inserted"] += articles_inserted

                logger.info(
                    f"Source {source}: {articles_inserted}/{articles_found} articles inserted"
                )

            except Exception as e:
                logger.error(f"Error running pipeline for {source}: {e}")
                results["results"][source] = {
                    "status": "error",
                    "message": str(e),
                    "articles_found": 0,
                    "articles_inserted": 0,
                }

        # Schedule periodic task if not already scheduled
        try:
            schedule_periodic_scraping(sources, interval_minutes, user_id)
        except Exception as e:
            logger.warning(f"Failed to schedule periodic scraping: {e}")
            results["scheduling_error"] = str(e)

        end_time = datetime.now(timezone.utc)
        results["task_completed_at"] = end_time.isoformat()
        results["total_elapsed_time"] = (end_time - start_time).total_seconds()

        logger.info(
            f"News scraping task completed: {results['total_inserted']}/{results['total_articles']} "
            f"articles inserted in {results['total_elapsed_time']:.2f} seconds"
        )

        return results

    except Exception as e:
        logger.error(f"News scraping task failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "task_started_at": datetime.now(timezone.utc).isoformat(),
        }


def schedule_periodic_scraping(sources, interval_minutes, user_id=None):
    """
    Schedule periodic news scraping using Celery Beat.

    Args:
        sources (list): List of source names to scrape
        interval_minutes (int): Interval in minutes
        user_id (str): Optional user ID for tracking
    """
    try:
        if celery is None:
            logger.warning("Celery not initialized, skipping periodic scheduling")
            return

        # Create a unique task name
        task_name = f"periodic_news_scraping_{interval_minutes}min"

        # Remove existing periodic task if it exists
        try:
            celery.control.cancel_task(task_name)
        except:
            pass  # Task might not exist

        # Schedule new periodic task
        celery.conf.beat_schedule[task_name] = {
            "task": "app.tasks.news_scraper_tasks.run_news_scrapers_task",
            "schedule": interval_minutes * 60,  # Convert minutes to seconds
            "args": (sources, interval_minutes, user_id),
        }

        logger.info(
            f"Scheduled periodic news scraping every {interval_minutes} minutes"
        )

    except Exception as e:
        logger.error(f"Failed to schedule periodic scraping: {e}")
        raise


def run_immediate_scraping(sources=None, user_id=None):
    """
    Run news scraping immediately without scheduling.

    Args:
        sources (list): List of source names to scrape
        user_id (str): Optional user ID for tracking

    Returns:
        dict: Results summary
    """
    return run_news_scrapers_task(
        sources=sources, interval_minutes=None, user_id=user_id
    )


async def run_pipelines_concurrently(sources=None, input_data=None, timeout_seconds=None, max_workers=None):
    """Run specified pipelines concurrently using asyncio and return structured results.

    Args:
        sources (list[str]|None): Source names to run. If None, run all in PIPELINE_MAP.
        input_data (dict|None): Payload to forward to each pipeline's run_pipeline.
        timeout_seconds (float|int|None): Optional timeout for all pipelines.
        max_workers (int|None): Not used in asyncio version (kept for API compatibility).

    Returns:
        dict: { source: {inserted_count, total_articles} | {error}, execution_time }
    """
    if not sources:
        sources = list(PIPELINE_MAP.keys())

    input_payload = input_data or {}

    logger.info(f"Concurrent run requested for sources: {sources}")

    start_time = time.perf_counter()
    results = {}

    async def _run_single_pipeline(source_name: str):
        """Run a single pipeline asynchronously and safely."""
        try:
            pipeline_class = PIPELINE_MAP.get(source_name)
            if not pipeline_class:
                return {"error": f"Source {source_name} not found"}
            
            logger.info(f"Submitting pipeline: {source_name}")
            
            # Run the synchronous pipeline in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, pipeline_class.run_pipeline, input_payload)
        except Exception as e:
            logger.exception(f"Error executing pipeline for {source_name}: {e}")
            return {"error": str(e)}

    # Create tasks for all pipelines
    tasks = {
        asyncio.create_task(_run_single_pipeline(source)): source 
        for source in sources
    }

    # Handle timeout if specified
    if timeout_seconds and isinstance(timeout_seconds, (int, float)) and timeout_seconds > 0:
        try:
            # Wait for all tasks with timeout
            done, pending = await asyncio.wait(
                tasks.keys(), 
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED
            )
            
            # Process completed tasks
            for task in done:
                source = tasks[task]
                try:
                    pipeline_result = await task
                    
                    if "error" in pipeline_result and "inserted_count" not in pipeline_result:
                         results[source] = pipeline_result
                         continue

                    inserted_count = pipeline_result.get("inserted_count", 0)
                    total_articles = pipeline_result.get("total_articles", 0)
                    results[source] = {
                        "inserted_count": inserted_count,
                        "total_articles": total_articles,
                    }
                    logger.info(
                        f"Pipeline {source} completed: inserted={inserted_count}, total={total_articles}"
                    )
                except Exception as e:
                    logger.exception(f"Pipeline {source} failed handling result")
                    results[source] = {"error": str(e)}
            
            # Handle timed out tasks
            for task in pending:
                source = tasks[task]
                task.cancel()
                logger.warning(f"Pipeline {source} timed out after {timeout_seconds} seconds")
                results[source] = {"error": "Timeout"}
                
        except Exception as e:
            logger.exception(f"Error during concurrent execution: {e}")
            for source in sources:
                if source not in results:
                    results[source] = {"error": str(e)}
    else:
        # No timeout - wait for all tasks to complete
        for task, source in tasks.items():
            try:
                pipeline_result = await task
                
                if "error" in pipeline_result and "inserted_count" not in pipeline_result:
                        results[source] = pipeline_result
                        continue

                inserted_count = pipeline_result.get("inserted_count", 0)
                total_articles = pipeline_result.get("total_articles", 0)
                results[source] = {
                    "inserted_count": inserted_count,
                    "total_articles": total_articles,
                }
                logger.info(
                    f"Pipeline {source} completed: inserted={inserted_count}, total={total_articles}"
                )
            except Exception as e:
                logger.exception(f"Pipeline {source} failed handling result")
                results[source] = {"error": str(e)}

    elapsed = time.perf_counter() - start_time
    results["execution_time"] = f"{elapsed:.2f} seconds"
    logger.info(f"Concurrent scraping finished in {elapsed:.2f} seconds")

    return results
