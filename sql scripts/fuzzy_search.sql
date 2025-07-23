CREATE OR REPLACE FUNCTION search_players_fuzzy(
    search_query TEXT,
    result_limit INTEGER DEFAULT 20,
    similarity_threshold DECIMAL DEFAULT 0.3
)
RETURNS TABLE (
    id UUID,
    nba_id INTEGER,
    name VARCHAR(255),
    team_name VARCHAR(255),
    team_abbreviation VARCHAR(10),
    position VARCHAR(10),
    search_rank REAL,
    similarity_score REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.id,
        p.nba_id,
        p.name,
        t.name as team_name,
        t.abbreviation as team_abbreviation,
        p.position,
        ts_rank(p.search_vector, plainto_tsquery('english', search_query)) as search_rank,
        similarity(p.name, search_query) as similarity_score
    FROM players p
    LEFT JOIN teams t ON p.team_id = t.id
    WHERE 
        p.is_active = true
        AND (
            p.search_vector @@ plainto_tsquery('english', search_query)
            OR similarity(p.name, search_query) > similarity_threshold
            OR p.name ILIKE '%' || search_query || '%'
        )
    ORDER BY 
        search_rank DESC,
        similarity_score DESC,
        p.name
    LIMIT result_limit;
END;
$$ LANGUAGE plpgsql;
$$ LANGUAGE plpgsql;