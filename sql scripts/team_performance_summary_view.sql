CREATE VIEW team_performance_summary AS
SELECT 
    t.id,
    t.name,
    t.abbreviation,
    ts.wins,
    ts.losses,
    ts.pct,
    ts.gb,
    ROUND(AVG(CASE WHEN ps.stat_key = 'pts' THEN ps.stat_value END), 1) as avg_points_scored,
    COUNT(DISTINCT p.id) as active_players,
    MAX(ps.game_date) as last_game_date
FROM teams t
LEFT JOIN team_standings ts ON t.id = ts.team_id
LEFT JOIN players p ON t.id = p.team_id AND p.is_active = true
LEFT JOIN player_stats ps ON p.id = ps.player_id
LEFT JOIN seasons s ON ts.season_id = s.id AND s.is_current = true
GROUP BY t.id, t.name, t.abbreviation, ts.wins, ts.losses, ts.pct, ts.gb;