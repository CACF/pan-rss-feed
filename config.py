import os
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# 1. DATABASE TABLE MODE SWITCHER (SINGLE SOURCE OF TRUTH)
# ==============================================================================
# Comment / uncomment ONE line below to switch all tables project-wide:

# DEFAULT_TABLE_NAME = "news_test"  # TESTING MODE
DEFAULT_TABLE_NAME = "news"  # PRODUCTION MODE (Default)

# Master Table Default (reads from .env if present, otherwise uses DEFAULT_TABLE_NAME above)
TABLE_NAME = os.getenv("DEFAULT_TABLE_NAME", DEFAULT_TABLE_NAME)


# ==============================================================================
# 2. CATEGORY & SYSTEM TABLE MAPPINGS
# (All categories automatically inherit TABLE_NAME unless explicitly overridden)
# ==============================================================================

BUSINESS_TABLE = os.getenv("BUSINESS_TABLE", TABLE_NAME)
SPORTS_TABLE = os.getenv("SPORTS_TABLE", TABLE_NAME)
FASHION_TABLE = os.getenv("FASHION_TABLE", TABLE_NAME)
ENTERTAINMENT_TABLE = os.getenv("ENTERTAINMENT_TABLE", TABLE_NAME)
FASHIONHUB_TABLE = os.getenv("FASHIONHUB_TABLE", TABLE_NAME)
HOUSTONPULSE_TABLE = os.getenv(
    "HOUSTONPULSE_TABLE", os.getenv("HOUSTANPULSE_TABLE", TABLE_NAME)
)
HOUSTANPULSE_TABLE = HOUSTONPULSE_TABLE
MEDIANEST_TABLE = os.getenv("MEDIANEST_TABLE", TABLE_NAME)
MEDIANESTDEV_TABLE = os.getenv("MEDIANESTDEV_TABLE", TABLE_NAME)
MERAMURREE_TABLE = os.getenv(
    "MERAMURREE_TABLE", os.getenv("MERAMUREE_TABLE", TABLE_NAME)
)
MERAMUREE_TABLE = MERAMURREE_TABLE
MERAPESHAWAR_TABLE = os.getenv("MERAPESHAWAR_TABLE", TABLE_NAME)
SPORTIFYHUB_TABLE = os.getenv("SPORTIFYHUB_TABLE", TABLE_NAME)
STYLEPULSE_TABLE = os.getenv("STYLEPULSE_TABLE", TABLE_NAME)
WAFAQ_TABLE = os.getenv("WAFAQ_TABLE", TABLE_NAME)


# ==============================================================================
# 3. SUPABASE DATABASE CREDENTIALS BY SYSTEM / CATEGORY
# ==============================================================================

# Main Database (Business, Sports, Entertainment, etc.)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Fashion Database
SUPABASE_FASHION_URL = os.getenv("SUPABASE_FASHION_URL", "")
SUPABASE_FASHION_KEY = os.getenv("SUPABASE_FASHION_KEY", "")

# Microservice Endpoints
SUPABASE_FASHIONHUB_URL = os.getenv("SUPABASE_FASHIONHUB_URL", "")
SUPABASE_FASHIONHUB_KEY = os.getenv("SUPABASE_FASHIONHUB_KEY", "")

SUPABASE_HOUSTONPULSE_URL = os.getenv("SUPABASE_HOUSTONPULSE_URL", "")
SUPABASE_HOUSTONPULSE_KEY = os.getenv("SUPABASE_HOUSTONPULSE_KEY", "")

SUPABASE_MEDIANEST_URL = os.getenv("SUPABASE_MEDIANEST_URL", "")
SUPABASE_MEDIANEST_KEY = os.getenv("SUPABASE_MEDIANEST_KEY", "")

SUPABASE_MEDIANESTDEV_URL = os.getenv("SUPABASE_MEDIANESTDEV_URL", "")
SUPABASE_MEDIANESTDEV_KEY = os.getenv("SUPABASE_MEDIANESTDEV_KEY", "")

SUPABASE_MERAMURREE_URL = os.getenv("SUPABASE_MERAMURREE_URL", "")
SUPABASE_MERAMURREE_KEY = os.getenv("SUPABASE_MERAMURREE_KEY", "")

SUPABASE_MERAPESHAWAR_URL = os.getenv("SUPABASE_MERAPESHAWAR_URL", "")
SUPABASE_MERAPESHAWAR_KEY = os.getenv("SUPABASE_MERAPESHAWAR_KEY", "")

SUPABASE_SPORTIFYHUB_URL = os.getenv("SUPABASE_SPORTIFYHUB_URL", "")
SUPABASE_SPORTIFYHUB_KEY = os.getenv("SUPABASE_SPORTIFYHUB_KEY", "")

SUPABASE_STYLEPULSE_URL = os.getenv("SUPABASE_STYLEPULSE_URL", "")
SUPABASE_STYLEPULSE_KEY = os.getenv("SUPABASE_STYLEPULSE_KEY", "")

SUPABASE_WAFAQ_URL = os.getenv("SUPABASE_WAFAQ_URL", "")
SUPABASE_WAFAQ_KEY = os.getenv("SUPABASE_WAFAQ_KEY", "")


# ==============================================================================
# 4. APPLICATION & SERVER CONFIGURATION
# ==============================================================================

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
PORT = int(os.getenv("PORT", 8000))
HOST = os.getenv("HOST", "0.0.0.0")
