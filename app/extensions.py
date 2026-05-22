import os
from supabase import create_client, Client

_supabase_client: Client = None


def get_supabase() -> Client:
    global _supabase_client
    if _supabase_client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY environment variables must be set"
            )
        _supabase_client = create_client(url, key)
    return _supabase_client


def init_supabase():
    """Initialize and validate Supabase connection."""
    get_supabase()
