CREATE VIEW current_season_performance AS
SELECT 
    p.id,
    p.name,
    p.position,
    t.name as team_name,
    t.abbreviation as team_abbr,
    COUNT(ps.id) as games_played,
    ROUND(AVG(CASE WHEN ps.stat_key = 'pts' THEN ps.stat_value END), 1) as avg_points,
    ROUND(AVG(CASE WHEN ps.stat_key = 'reb' THEN ps.stat_value END), 1) as avg_rebounds,
    ROUND(AVG(CASE WHEN ps.stat_key = 'ast' THEN ps.stat_value END), 1) as avg_assists,
    ROUND(AVG(CASE WHEN ps.stat_key = 'min' THEN ps.stat_value END), 1) as avg_minutes,
    MAX(ps.game_date) as last_game
FROM players p
LEFT JOIN teams t ON p.team_id = t.id
LEFT JOIN player_stats ps ON p.id = ps.player_id
LEFT JOIN seasons s ON ps.season_id = s.id
WHERE s.is_current = true AND p.is_active = true
GROUP BY p.id, p.name, p.position, t.name, t.abbreviation;

-- Index the view for better performance
CREATE INDEX idx_current_season_performance_points ON player_stats(stat_value DESC) 
    WHERE stat_key = 'pts' AND season_id IN (SELECT id FROM seasons WHERE is_current = true);