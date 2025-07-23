CREATE OR REPLACE FUNCTION get_league_leaders(
    stat_key_param VARCHAR(20),
    season_year_param INTEGER DEFAULT EXTRACT(YEAR FROM CURRENT_DATE),
    result_limit INTEGER DEFAULT 20,
    min_games INTEGER DEFAULT 10
)
RETURNS TABLE (
    player_id UUID,
    player_name VARCHAR(255),
    team_name VARCHAR(255),
    team_abbreviation VARCHAR(10),
    avg_value DECIMAL,
    total_value DECIMAL,
    games_played BIGINT,
    last_game_date DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.id as player_id,
        p.name as player_name,
        t.name as team_name,
        t.abbreviation as team_abbreviation,
        ROUND(AVG(ps.stat_value), 2) as avg_value,
        ROUND(SUM(ps.stat_value), 2) as total_value,
        COUNT(ps.id) as games_played,
        MAX(ps.game_date) as last_game_date
    FROM players p
    JOIN player_stats ps ON p.id = ps.player_id
    JOIN seasons s ON ps.season_id = s.id
    LEFT JOIN teams t ON p.team_id = t.id
    WHERE 
        p.is_active = true
        AND ps.stat_key = stat_key_param
        AND s.year = season_year_param
        AND ps.stat_value IS NOT NULL
    GROUP BY p.id, p.name, t.name, t.abbreviation
    HAVING COUNT(ps.id) >= min_games
    ORDER BY avg_value DESC
    LIMIT result_limit;
END;
$$ LANGUAGE plpgsql;