-- Your table already exists. Run only the indexes and functions below.
-- If starting fresh, the CREATE TABLE is included for reference.

-- CREATE TABLE IF NOT EXISTS news (
--   id text PRIMARY KEY,        -- article URL (unique identifier)
--   title text,
--   content text,
--   authors text,
--   tags text[],
--   image text,
--   created_at timestamptz,     -- article publish date
--   source text,
--   genre text,
--   language text,
--   media_origin text
-- );

-- Indexes
CREATE INDEX IF NOT EXISTS idx_news_source_genre_date ON news(source, genre, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_source ON news(source);
CREATE INDEX IF NOT EXISTS idx_news_created_at ON news(created_at DESC);

-- -----------------------------------------------
-- RPC: get all distinct sources
-- -----------------------------------------------
CREATE OR REPLACE FUNCTION get_distinct_sources()
RETURNS TABLE(source TEXT)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT source FROM news WHERE source IS NOT NULL ORDER BY source;
$$;

-- -----------------------------------------------
-- RPC: get distinct genres for a specific source
-- -----------------------------------------------
CREATE OR REPLACE FUNCTION get_distinct_genres_for_source(p_source TEXT)
RETURNS TABLE(genre TEXT)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT genre FROM news WHERE source = p_source AND genre IS NOT NULL ORDER BY genre;
$$;

-- -----------------------------------------------
-- RPC: filtered news query
-- -----------------------------------------------
CREATE OR REPLACE FUNCTION get_filtered_news(
    p_search TEXT DEFAULT NULL,
    p_sources TEXT[] DEFAULT NULL,
    p_genres TEXT[] DEFAULT NULL,
    p_start_date TIMESTAMPTZ DEFAULT NULL,
    p_end_date TIMESTAMPTZ DEFAULT NULL,
    p_max_articles INTEGER DEFAULT NULL
)
RETURNS TABLE (
    id TEXT,
    title TEXT,
    content TEXT,
    authors TEXT,
    tags TEXT[],
    image TEXT,
    created_at TIMESTAMPTZ,
    source TEXT,
    genre TEXT,
    language TEXT,
    media_origin TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        n.id,
        n.title,
        n.content,
        n.authors,
        n.tags,
        n.image,
        n.created_at,
        n.source,
        n.genre,
        n.language,
        n.media_origin
    FROM news n
    WHERE
        (
            p_search IS NULL
            OR n.title ILIKE '%' || p_search || '%'
            OR n.content ILIKE '%' || p_search || '%'
            OR EXISTS (
                SELECT 1
                FROM unnest(n.tags) AS t(tag)
                WHERE t.tag ILIKE '%' || p_search || '%'
            )
        )
        AND (p_sources IS NULL OR n.source = ANY(p_sources))
        AND (
            p_genres IS NULL
            OR EXISTS (
                SELECT 1
                FROM unnest(p_genres) AS g(genre)
                WHERE n.genre ILIKE g.genre
            )
        )
        AND (p_start_date IS NULL OR n.created_at >= p_start_date)
        AND (p_end_date IS NULL OR n.created_at <= p_end_date)
    ORDER BY n.created_at DESC NULLS LAST
    LIMIT CASE
        WHEN p_max_articles IS NOT NULL AND p_max_articles > 0 THEN p_max_articles
        ELSE NULL
    END;
END;
$$;

-- -----------------------------------------------
-- RPC: delete articles older than 7 days
-- -----------------------------------------------
CREATE OR REPLACE FUNCTION delete_old_news()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM news
    WHERE created_at < NOW() - INTERVAL '7 days';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$;
