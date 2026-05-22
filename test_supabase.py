"""
Quick test script — run with: python test_supabase.py
Tests:
  1. Supabase connection
  2. Insert one dummy article
  3. Query it back via RPC
  4. Clean up
"""
from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timezone
from app.extensions import get_supabase

supabase = get_supabase()

# ── 1. Connection check ───────────────────────────────────────────────────────
print("✓ Connected to Supabase")

# ── 2. Insert a dummy article ─────────────────────────────────────────────────
dummy = {
    "id": "https://test.example.com/dummy-article",
    "title": "Test Article",
    "content": "This is a test article with enough words to pass validation.",
    "authors": "Test Author",
    "tags": ["test", "supabase"],
    "image": None,
    "created_at": datetime.now(timezone.utc).isoformat(),
    "source": "TEST",
    "genre": "Technology",
    "language": "en-us",
    "media_origin": "foreign",
}

result = supabase.table("news").upsert(dummy, on_conflict="id").execute()
print(f"✓ Inserted article: {result.data[0]['id']}")

# ── 3. Query via RPC ──────────────────────────────────────────────────────────
rpc_result = supabase.rpc("get_filtered_news", {
    "p_search": "Test Article",
    "p_sources": None,
    "p_genres": None,
    "p_start_date": None,
    "p_end_date": None,
    "p_max_articles": 5,
}).execute()
print(f"✓ RPC returned {len(rpc_result.data)} article(s)")
if rpc_result.data:
    print(f"  → title: {rpc_result.data[0]['title']}")

# ── 4. Sources RPC ────────────────────────────────────────────────────────────
sources = supabase.rpc("get_distinct_sources", {}).execute()
print(f"✓ Distinct sources: {[r['source'] for r in sources.data]}")

# ── 5. Clean up ───────────────────────────────────────────────────────────────
supabase.table("news").delete().eq("id", dummy["id"]).execute()
print("✓ Cleaned up dummy article")

print("\n✅ All tests passed — Supabase integration is working!")
