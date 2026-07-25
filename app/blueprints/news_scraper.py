import logging
from flask import Blueprint, jsonify
import run_scrapers

logger = logging.getLogger(__name__)

bp = Blueprint("news_scraper", __name__)


@bp.route("/run_scrapers", methods=["POST"])
def run_scrapers_endpoint():
    """Run registered category news scraping pipelines concurrently."""
    try:
        logger.info("Executing news scraping pipelines via API endpoint")
        run_scrapers.main()
        return jsonify({"status": "success", "message": "Scraping pipelines executed successfully"}), 200
    except Exception as e:
        logger.error(f"Error running scrapers endpoint: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500
