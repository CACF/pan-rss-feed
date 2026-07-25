# PAN RSS Feed Scraper

A category-based, 2-level multithreaded news scraper system with global configuration and Supabase integration.

## Project Structure
```text
pan-rss-feed/
├── config.py                 # Single global configuration (Table names, Supabase credentials)
├── pipeline_registry.py      # Central registry listing enabled category pipelines
├── run_scrapers.py           # Main executable script for running all scrapers
├── runFlask.py               # Flask application server
├── startApp.bat              # Windows launch script
├── pipeline_maps/            # Category scraper registries
│   ├── business_pipeline_map.py
│   ├── sports_pipeline_map.py
│   ├── fashion_pipeline_map.py
│   └── entertainment_pipeline_map.py
├── scrapers/                 # Scraper modules organized by category
│   ├── business/
│   │   ├── pipeline.py       # BusinessPipeline
│   │   └── [40 Scrapers]
│   ├── sports/
│   │   ├── pipeline.py       # SportsPipeline
│   │   └── [19 Scrapers]
│   ├── fashion/
│   │   ├── pipeline.py       # FashionPipeline
│   │   └── [8 Scrapers]
│   └── entertainment/
│       └── pipeline.py       # EntertainmentPipeline
└── app/                      # Application blueprints & utilities
    ├── __init__.py
    ├── utilities.py
    ├── blueprints/
    └── utils/
        └── supabase_client.py
```

## Running Scrapers
To run all enabled scrapers:
```bash
python run_scrapers.py
```

## Testing Specific Categories or Scrapers
- **To test specific categories**: Edit `pipeline_registry.py` to comment/uncomment category pipelines.
- **To test individual scrapers**: Edit `pipeline_maps/<category>_pipeline_map.py` to comment/uncomment specific scrapers.

## Switching Between Test and Production
Edit table names in global `config.py` (Line 9):
```python
DEFAULT_TABLE_NAME = "test_news"  # Change to "news" for production
```