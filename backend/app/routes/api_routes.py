# Update backend/app/routes/api_routes.py to fix the missing endpoints

"""
API routes for AJAX requests and mobile app support.
"""

from flask import Blueprint, request, jsonify
from app.auth.decorators import login_required, permission_required
from app.auth.auth_service import get_current_user
from app.services.search_service import search_service
from app.services.stats_service import stats_service
from app.services.bets_service import betting_service
from app.db.supabase import supabase
from app.core.logging import logger

api_bp = Blueprint('api', __name__)

# Add the missing routes that are being called

@api_bp.route('/players')
def get_players():
    """API endpoint to get all players."""
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = int(request.args.get('offset', 0))
        
        result = supabase.table('players').select(
            '*, teams(name, abbreviation)'
        ).range(offset, offset + limit - 1).execute()
        
        return jsonify({'players': result.data if result.data else []})
        
    except Exception as e:
        logger.error(f"Get players API error: {e}")
        return jsonify({'error': 'Failed to get players'}), 500

@api_bp.route('/players/all')
def get_all_players():
    """API endpoint to get all active players."""
    try:
        result = supabase.table('players').select(
            'id, name, position, teams(name, abbreviation)'
        ).eq('is_active', True).execute()
        
        return jsonify({'players': result.data if result.data else []})
        
    except Exception as e:
        logger.error(f"Get all players API error: {e}")
        return jsonify({'error': 'Failed to get players'}), 500

@api_bp.route('/games')
def get_games():
    """API endpoint to get games."""
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
        
        # For now, return empty array since we don't have games data yet
        return jsonify({'games': []})
        
    except Exception as e:
        logger.error(f"Get games API error: {e}")
        return jsonify({'error': 'Failed to get games'}), 500

@api_bp.route('/bets')
@login_required
def get_bets():
    """API endpoint to get user's bets."""
    try:
        user = get_current_user()
        status = request.args.get('status')
        limit = min(int(request.args.get('limit', 20)), 100)
        
        bets = betting_service.get_user_bets(user['user_id'], status=status, limit=limit)
        
        return jsonify({'bets': bets})
        
    except Exception as e:
        logger.error(f"Get bets API error: {e}")
        return jsonify({'error': 'Failed to get bets'}), 500

@api_bp.route('/watchlist')
@login_required
def get_watchlist():
    """API endpoint to get user's watchlist."""
    try:
        user = get_current_user()
        
        result = supabase.table('watchlists').select(
            '*, players(name, position, teams(name, abbreviation))'
        ).eq('user_id', user['user_id']).execute()
        
        return jsonify({'watchlist': result.data if result.data else []})
        
    except Exception as e:
        logger.error(f"Get watchlist API error: {e}")
        return jsonify({'error': 'Failed to get watchlist'}), 500

# Keep the existing routes below...

@api_bp.route('/search/players')
def search_players():
    """API endpoint for player search."""
    try:
        query = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 10)), 50)
        
        if not query:
            return jsonify({'players': []})
        
        players = search_service.unified_player_search(query, limit=limit)
        return jsonify({'players': players})
        
    except Exception as e:
        logger.error(f"Player search API error: {e}")
        return jsonify({'error': 'Search failed'}), 500

@api_bp.route('/search/suggestions')
def search_suggestions():
    """API endpoint for search suggestions."""
    try:
        query = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 5)), 10)
        
        if len(query) < 2:
            return jsonify({'suggestions': []})
        
        suggestions = search_service.get_search_suggestions(query, limit=limit)
        return jsonify({'suggestions': suggestions})
        
    except Exception as e:
        logger.error(f"Search suggestions API error: {e}")
        return jsonify({'error': 'Failed to get suggestions'}), 500

@api_bp.route('/players/<player_id>/stats')
def player_stats(player_id):
    """API endpoint for player statistics."""
    try:
        season_year = request.args.get('season', 2025, type=int)
        
        stats = stats_service.get_player_season_stats(player_id, season_year)
        
        if not stats:
            return jsonify({'error': 'Player stats not found'}), 404
        
        return jsonify({'stats': stats})
        
    except Exception as e:
        logger.error(f"Player stats API error: {e}")
        return jsonify({'error': 'Failed to get player stats'}), 500

@api_bp.route('/betting/odds')
@login_required
@permission_required('bets.create')
def betting_odds():
    """API endpoint for betting odds calculation."""
    try:
        player_id = request.args.get('player_id')
        stat_key = request.args.get('stat_key')
        threshold = float(request.args.get('threshold', 0))
        
        if not all([player_id, stat_key, threshold]):
            return jsonify({'error': 'Missing required parameters'}), 400
        
        odds_data = betting_service.calculate_odds(player_id, stat_key, threshold)
        
        if 'error' in odds_data:
            return jsonify(odds_data), 400
        
        return jsonify(odds_data)
        
    except Exception as e:
        logger.error(f"Betting odds API error: {e}")
        return jsonify({'error': 'Failed to calculate odds'}), 500

@api_bp.route('/betting/place', methods=['POST'])
@login_required
@permission_required('bets.create')
def place_bet():
    """API endpoint to place a bet."""
    try:
        user = get_current_user()
        data = request.get_json()
        
        required_fields = ['player_id', 'stat_key', 'threshold', 'side', 'stake']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        result = betting_service.place_bet(
            user_id=user['user_id'],
            player_id=data['player_id'],
            stat_key=data['stat_key'],
            threshold=float(data['threshold']),
            side=data['side'],
            stake=float(data['stake'])
        )
        
        if 'error' in result:
            return jsonify(result), 400
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Place bet API error: {e}")
        return jsonify({'error': 'Failed to place bet'}), 500

@api_bp.route('/betting/simulate/<bet_id>', methods=['POST'])
@login_required
def simulate_bet(bet_id):
    """API endpoint to simulate bet outcome."""
    try:
        result = betting_service.simulate_bet_outcome(bet_id)
        
        if 'error' in result:
            return jsonify(result), 400
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Simulate bet API error: {e}")
        return jsonify({'error': 'Failed to simulate bet'}), 500

@api_bp.route('/betting/resolve-all', methods=['POST'])
@login_required
def resolve_all_bets():
    """API endpoint to resolve all pending bets (simulation)."""
    try:
        result = betting_service.auto_resolve_pending_bets()
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Resolve all bets API error: {e}")
        return jsonify({'error': 'Failed to resolve bets'}), 500

@api_bp.route('/watchlist', methods=['POST'])
@login_required
@permission_required('watchlist.create')
def add_to_watchlist():
    """API endpoint to add player to watchlist."""
    try:
        user = get_current_user()
        data = request.get_json()
        
        player_id = data.get('player_id')
        priority = data.get('priority', 3)
        notes = data.get('notes', '')
        
        if not player_id:
            return jsonify({'error': 'Player ID required'}), 400
        
        # Check watchlist limit based on user tier
        profile = user.get('profile', {})
        watchlist_limit = profile.get('watchlist_limit', 10)
        
        # Count current watchlist items
        count_result = supabase.table('watchlists').select(
            'id', count='exact'
        ).eq('user_id', user['user_id']).execute()
        
        current_count = count_result.count or 0
        
        if current_count >= watchlist_limit:
            return jsonify({
                'error': f'Watchlist limit reached ({watchlist_limit}). Upgrade for more.'
            }), 400
        
        # Add to watchlist
        result = supabase.table('watchlists').insert({
            'user_id': user['user_id'],
            'player_id': player_id,
            'priority': priority,
            'notes': notes
        }).execute()
        
        if result.error:
            return jsonify({'error': 'Failed to add to watchlist'}), 500
        
        return jsonify({'success': True, 'message': 'Added to watchlist'})
        
    except Exception as e:
        logger.error(f"Add to watchlist API error: {e}")
        return jsonify({'error': 'Failed to add to watchlist'}), 500

@api_bp.route('/watchlist/<item_id>', methods=['DELETE'])
@login_required
def remove_from_watchlist(item_id):
    """API endpoint to remove item from watchlist."""
    try:
        user = get_current_user()
        
        result = supabase.table('watchlists').delete().eq(
            'id', item_id
        ).eq('user_id', user['user_id']).execute()
        
        if result.error:
            return jsonify({'error': 'Failed to remove from watchlist'}), 500
        
        return jsonify({'success': True, 'message': 'Removed from watchlist'})
        
    except Exception as e:
        logger.error(f"Remove from watchlist API error: {e}")
        return jsonify({'error': 'Failed to remove from watchlist'}), 500
        
@api_bp.route('/watchlist', methods=['GET'])
@login_required
def get_watchlist():
    """API endpoint to get user's watchlist."""
    try:
        user = get_current_user()
        
        result = supabase.table('watchlists').select(
            '*, players(name, position, teams(name, abbreviation))'
        ).eq('user_id', user['user_id']).execute()
        
        return jsonify({'watchlist': result.data if result.data else []})
        
    except Exception as e:
        logger.error(f"Get watchlist API error: {e}")
        return jsonify({'error': 'Failed to get watchlist'}), 500



@api_bp.route('/league-leaders/<stat_key>')
def league_leaders(stat_key):
    """API endpoint for league leaders."""
    try:
        limit = min(int(request.args.get('limit', 20)), 50)
        season_year = request.args.get('season', 2025, type=int)
        
        leaders = stats_service.get_league_leaders(stat_key, season_year, limit)
        return jsonify({'leaders': leaders})
        
    except Exception as e:
        logger.error(f"League leaders API error: {e}")
        return jsonify({'error': 'Failed to get league leaders'}), 500