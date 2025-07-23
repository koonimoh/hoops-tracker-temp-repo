# backend/app/routes/main_routes.py
"""
Main application routes for pages and views.
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from app.auth.decorators import login_required
from app.auth.auth_service import get_current_user
from app.services.search_service import search_service
from app.services.stats_service import stats_service
from app.services.bets_service import betting_service
from app.db.supabase import supabase
from app.core.logging import logger

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """Home page - redirect to dashboard if logged in."""
    user = get_current_user()
    if user:
        return redirect(url_for('main.dashboard'))
    
    # Get some sample data for homepage
    try:
        # Get top performers
        top_scorers = stats_service.get_league_leaders('pts', limit=5)
        top_rebounders = stats_service.get_league_leaders('reb', limit=5)
        
        return render_template('index.html', 
                             top_scorers=top_scorers,
                             top_rebounders=top_rebounders)
    except Exception as e:
        logger.error(f"Error loading homepage data: {e}")
        return render_template('index.html')

@main_bp.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard for logged-in users."""
    try:
        user = get_current_user()
        
        # Get user's recent bets
        recent_bets = betting_service.get_user_bets(user['user_id'], limit=5)
        
        # Get betting statistics
        bet_stats = betting_service.get_betting_statistics(user['user_id'])
        
        # Get user's watchlist
        watchlist_result = supabase.table('watchlists').select(
            '*, players(name, position, teams(name, abbreviation))'
        ).eq('user_id', user['user_id']).limit(10).execute()
        
        watchlist = watchlist_result.data if watchlist_result.data else []
        
        # Get league leaders
        top_scorers = stats_service.get_league_leaders('pts', limit=10)
        
        return render_template('dashboard.html',
                             user=user,
                             recent_bets=recent_bets,
                             bet_stats=bet_stats,
                             watchlist=watchlist,
                             top_scorers=top_scorers)
                             
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return render_template('dashboard.html', error="Failed to load dashboard data")

@main_bp.route('/players')
def players():
    """Players listing page."""
    try:
        # Get search query
        search_query = request.args.get('search', '')
        page = int(request.args.get('page', 1))
        per_page = 20
        
        if search_query:
            # Use search service
            players_data = search_service.unified_player_search(search_query, limit=per_page)
        else:
            # Get all active players
            offset = (page - 1) * per_page
            result = supabase.table('players').select(
                '*, teams(name, abbreviation)'
            ).eq('is_active', True).range(offset, offset + per_page - 1).execute()
            
            players_data = result.data if result.data else []
        
        return render_template('players.html', 
                             players=players_data,
                             search_query=search_query,
                             page=page)
                             
    except Exception as e:
        logger.error(f"Players page error: {e}")
        return render_template('players.html', players=[], error="Failed to load players")

@main_bp.route('/players/<player_id>')
def player_detail(player_id):
    """Individual player detail page."""
    try:
        # Get player info
        player_result = supabase.table('players').select(
            '*, teams(name, abbreviation, city)'
        ).eq('id', player_id).single().execute()
        
        if player_result.error or not player_result.data:
            return render_template('player_detail.html', error="Player not found")
        
        player = player_result.data
        
        # Get player season stats
        season_stats = stats_service.get_player_season_stats(player_id)
        
        # Get recent games (last 10)
        recent_games_result = supabase.table('player_stats').select(
            'game_date, stat_key, stat_value, opponent_team_id, teams(name)'
        ).eq('player_id', player_id).order('game_date', desc=True).limit(50).execute()
        
        # Process recent games data
        recent_games = []
        if recent_games_result.data:
            games_by_date = {}
            for stat in recent_games_result.data:
                date = stat['game_date']
                if date not in games_by_date:
                    games_by_date[date] = {
                        'game_date': date,
                        'opponent': stat.get('teams', {}).get('name', 'Unknown'),
                        'stats': {}
                    }
                games_by_date[date]['stats'][stat['stat_key']] = stat['stat_value']
            
            # Convert to list and add calculated fields
            for game_data in games_by_date.values():
                stats = game_data['stats']
                game_data['points'] = stats.get('pts', 0)
                game_data['rebounds'] = stats.get('reb', 0)
                game_data['assists'] = stats.get('ast', 0)
                recent_games.append(game_data)
            
            recent_games = sorted(recent_games, key=lambda x: x['game_date'], reverse=True)[:10]
        
        return render_template('player_detail.html',
                             player=player,
                             season_stats=season_stats,
                             recent_games=recent_games)
                             
    except Exception as e:
        logger.error(f"Player detail error: {e}")
        return render_template('player_detail.html', error="Failed to load player data")

@main_bp.route('/bets')
@login_required
def bets():
    """Betting page."""
    try:
        user = get_current_user()
        
        # Get user's bets
        user_bets = betting_service.get_user_bets(user['user_id'], limit=50)
        
        # Get betting statistics
        bet_stats = betting_service.get_betting_statistics(user['user_id'])
        
        # Get popular players for betting
        popular_players_result = supabase.table('players').select(
            '*, teams(name, abbreviation)'
        ).eq('is_active', True).limit(20).execute()
        
        popular_players = popular_players_result.data if popular_players_result.data else []
        
        return render_template('bets.html',
                             bets=user_bets,
                             stats=bet_stats,
                             popular_players=popular_players)
                             
    except Exception as e:
        logger.error(f"Bets page error: {e}")
        return render_template('bets.html', bets=[], stats={})

@main_bp.route('/watchlist')
@login_required
def watchlist():
    """User's watchlist page."""
    try:
        user = get_current_user()
        
        # Get user's watchlist with player data
        watchlist_result = supabase.table('watchlists').select(
            '''*, players(*, teams(name, abbreviation)),
               user_profiles!inner(subscription_tier)'''
        ).eq('user_id', user['user_id']).order('added_at', desc=True).execute()
        
        watchlist_data = watchlist_result.data if watchlist_result.data else []
        
        return render_template('watchlist.html', watchlist_players=watchlist_data)
        
    except Exception as e:
        logger.error(f"Watchlist page error: {e}")
        return render_template('watchlist.html', watchlist_players=[])

@main_bp.route('/standings')
def standings():
    """NBA standings page."""
    try:
        # Get current season standings
        standings_result = supabase.table('team_standings').select(
            '*, teams(name, abbreviation, conference), seasons!inner(is_current)'
        ).eq('seasons.is_current', True).execute()
        
        if standings_result.data:
            standings = standings_result.data
            
            # Separate by conference
            eastern_standings = [s for s in standings if s['teams']['conference'] == 'East']
            western_standings = [s for s in standings if s['teams']['conference'] == 'West']
            
            # Sort by winning percentage
            eastern_standings.sort(key=lambda x: x['pct'], reverse=True)
            western_standings.sort(key=lambda x: x['pct'], reverse=True)
        else:
            eastern_standings = []
            western_standings = []
        
        return render_template('standings.html',
                             eastern_standings=eastern_standings,
                             western_standings=western_standings)
                             
    except Exception as e:
        logger.error(f"Standings page error: {e}")
        return render_template('standings.html', 
                             eastern_standings=[], 
                             western_standings=[])