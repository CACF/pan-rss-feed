import logging
from flask import Blueprint, jsonify, request
from app.news_sources.pipeline_map import PIPELINE_MAP
from app.tasks.news_scraper_tasks import run_pipelines_concurrently

logger = logging.getLogger(__name__)

bp = Blueprint("news_scraper", __name__)
@bp.route("/run_scrapers", methods=["POST"])
def run_scrapers():
    """Run all requested news scraping pipelines concurrently.

    Request JSON (optional):
    {
        "sources": ["BBC", "WSJ"],
        "input_data": {"email": "user@example.com"}
    }
    """
    payload = request.get_json(silent=True) or {}

    requested_sources = payload.get("sources")
    if requested_sources:
        sources = [s for s in requested_sources if s in PIPELINE_MAP]
        unknown = [s for s in requested_sources if s not in PIPELINE_MAP]
        for s in unknown:
            logger.warning(f"Unknown source requested: {s}")
    else:
        sources = list(PIPELINE_MAP.keys())

    input_data = payload.get("input_data") or {}
    timeout_seconds = payload.get("timeout_seconds")

    if not sources:
        return jsonify({"error": "No valid sources provided"}), 400

    logger.info(f"Starting concurrent scraping for sources: {sources}")
    results = run_pipelines_concurrently(
        sources=sources,
        input_data=input_data,
        timeout_seconds=timeout_seconds,
    )
    return jsonify(results), 200
