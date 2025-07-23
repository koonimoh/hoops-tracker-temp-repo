CREATE OR REPLACE FUNCTION get_player_stats_advanced(
    player_uuid UUID,
    stat_keys TEXT[] DEFAULT ARRAY['pts', 'reb', 'ast'],
    season_year INTEGER DEFAULT EXTRACT(YEAR FROM CURRENT_DATE),
    game_limit INTEGER DEFAULT 20
)
RETURNS TABLE (
    game_date DATE,
    opponent VARCHAR(255),
    is_home BOOLEAN,
    minutes_played DECIMAL,
    stats JSONB,
    efficiency_rating DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ps.game_date,
        opp.name as opponent,
        ps.is_home,
        MAX(CASE WHEN ps.stat_key = 'min' THEN ps.stat_value END) as minutes_played,
        jsonb_object_agg(ps.stat_key, ps.stat_value) as stats,
        MAX(ps.efficiency_rating) as efficiency_rating
    FROM player_stats ps
    JOIN seasons s ON ps.season_id = s.id
    LEFT JOIN teams opp ON ps.opponent_team_id = opp.id
    WHERE 
        ps.player_id = player_uuid
        AND s.year = season_year
        AND ps.stat_key = ANY(stat_keys)
        AND ps.game_date IS NOT NULL
    GROUP BY ps.game_date, opp.name, ps.is_home, ps.game_id
    ORDER BY ps.game_date DESC
    LIMIT game_limit;
END;
$$ LANGUAGE plpgsql;