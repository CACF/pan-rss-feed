import os
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# 1. DATABASE TABLE MODE SWITCHER (SINGLE SOURCE OF TRUTH)
# ==============================================================================
# Change TABLE_PREFIX to "news" for production mode, or "news_test" for testing mode.
TABLE_PREFIX = "news"

# Explicit table mapping for each category / system
TABLES = {
    "business": f"{TABLE_PREFIX}",
    "sports": f"{TABLE_PREFIX}",
    "fashion": f"{TABLE_PREFIX}",
    "houstonpulse": f"{TABLE_PREFIX}",
    "meramurree": f"{TABLE_PREFIX}",
    "wafaq": f"{TABLE_PREFIX}",
    "entertainment": f"{TABLE_PREFIX}",
}

# Individual table constants for scrapers importing them directly
BUSINESS_TABLE = TABLES["business"]
SPORTS_TABLE = TABLES["sports"]
FASHION_TABLE = TABLES["fashion"]
HOUSTONPULSE_TABLE = TABLES["houstonpulse"]
MERAMURREE_TABLE = TABLES["meramurree"]
WAFAQ_TABLE = TABLES["wafaq"]
ENTERTAINMENT_TABLE = TABLES["entertainment"]


# ==============================================================================
# 2. SUPABASE DATABASE CREDENTIALS BY SYSTEM
# ==============================================================================

# Business / MediaNest Database
SUPABASE_MEDIANEST_URL = os.getenv("SUPABASE_MEDIANEST_URL", "")
SUPABASE_MEDIANEST_KEY = os.getenv("SUPABASE_MEDIANEST_KEY", "")

# Sports Database
SUPABASE_SPORTS_URL = os.getenv("SUPABASE_SPORTS_URL", "")
SUPABASE_SPORTS_KEY = os.getenv("SUPABASE_SPORTS_KEY", "")

# Fashion Database
SUPABASE_FASHION_URL = os.getenv("SUPABASE_FASHION_URL", "")
SUPABASE_FASHION_KEY = os.getenv("SUPABASE_FASHION_KEY", "")

# HoustonPulse Database
SUPABASE_HOUSTONPULSE_URL = os.getenv("SUPABASE_HOUSTONPULSE_URL", "")
SUPABASE_HOUSTONPULSE_KEY = os.getenv("SUPABASE_HOUSTONPULSE_KEY", "")

# MeraMurree Database
SUPABASE_MERAMURREE_URL = os.getenv("SUPABASE_MERAMURREE_URL", "")
SUPABASE_MERAMURREE_KEY = os.getenv("SUPABASE_MERAMURREE_KEY", "")

# Wafaq Database
SUPABASE_WAFAQ_URL = os.getenv("SUPABASE_WAFAQ_URL", "")
SUPABASE_WAFAQ_KEY = os.getenv("SUPABASE_WAFAQ_KEY", "")


# ==============================================================================
# 3. APPLICATION & SERVER CONFIGURATION
# ==============================================================================

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
PORT = int(os.getenv("PORT", 8000))
HOST = os.getenv("HOST", "0.0.0.0")
