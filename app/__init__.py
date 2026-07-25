import os
import logging
from flask import Flask
from flask_cors import CORS

logger = logging.getLogger(__name__)


def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
    
    # Initialize extensions
    CORS(app, resources={r"/*": {"origins": "*"}})
    
    # Register blueprints
    from app.blueprints import news_scraper
    app.register_blueprint(news_scraper.bp)
    
    logger.info("Flask app initialized successfully")
    return app
