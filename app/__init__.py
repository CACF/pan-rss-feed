import os
import logging
from flask import Flask
from flask_cors import CORS
from app.extensions import mongo, build_db_uri

logger = logging.getLogger(__name__)


def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
    
    # MongoDB Configuration
    db_uri = build_db_uri(
        DB_USER=os.getenv('DB_USER'),
        DB_PW=os.getenv('DB_PW'),
        DB_HOST=os.getenv('DB_HOST'),
        DB_PORT=int(os.getenv('DB_PORT', 27017)),
        DB_NAME=os.getenv('DB_NAME', 'Karobaar')
    )
    app.config['MONGO_URI'] = db_uri
    
    # Initialize extensions
    mongo.init_app(app)
    CORS(app, resources={r"/*": {"origins": "*"}})
    
    # Register blueprints
    from app.blueprints import news, news_scraper
    app.register_blueprint(news.bp)
    app.register_blueprint(news_scraper.bp)
    
    logger.info("Flask app initialized successfully")
    return app
