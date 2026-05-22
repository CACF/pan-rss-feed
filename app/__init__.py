import os
import logging
from flask import Flask
from flask_cors import CORS
from app.extensions import init_supabase

logger = logging.getLogger(__name__)


def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__)

    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

    # Initialize Supabase
    init_supabase()

    CORS(app, resources={r"/*": {"origins": "*"}})

    # Register blueprints
    from app.blueprints import news, news_scraper
    app.register_blueprint(news.bp)
    app.register_blueprint(news_scraper.bp)

    logger.info("Flask app initialized successfully")
    return app
