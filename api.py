from flask import Blueprint, request, jsonify, current_app
from auth import require_auth, get_current_user, require_role
import logging

# TODO: Add rate limiting to prevent API abuse
# FIXME: Some endpoints might need better error handling
# Main API routes for the Hoops Tracker application
# This handles all the REST endpoints for frontend communication
# Create API blueprint
api_bp = Blueprint('api', __name__)

# Added pagination here because loading all players at once was too slow
# per_page is limited to 100 to prevent memory issues
@api_bp.route('/players')
def get_players():
    """Get paginated list of players with enhanced filtering"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)  # Max 100 per page
        search = request.args.get('search', '')
        team_id = request.args.get('team_id', type=int)
        position = request.args.get('position', '')
        
        result = current_app.supabase.get_players_paginated(
            page=page,
            per_page=per_page,
            search=search,
            team_id=team_id,
            position=position
        )
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logging.error(f"API get players error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Enhanced this endpoint to include season stats and recent games
# Had to add better error handling because some players don't have stats yet
# TODO: Cache this data since player details are accessed frequently
@api_bp.route('/players/<int:player_id>')
def get_player_enhanced(player_id):
    """Get player details with enhanced stats and better error handling"""
    try:
        player = current_app.supabase.get_player_by_id(player_id)
        
        if not player:
            return jsonify({
                'success': False,
                'error': 'Player not found'
            }), 404
        
        # Get additional data 
        season_stats = None
        recent_games = []
        
        try:
            season_stats = current_app.supabase.get_player_season_stats(player_id)
            logging.info(f"Retrieved season stats for player {player_id}: {season_stats}")
        except Exception as stats_error:
            logging.warning(f"Could not fetch season stats for player {player_id}: {stats_error}")
        
        try:
            recent_games = current_app.supabase.get_player_recent_games(player_id, limit=10)
            logging.info(f"Retrieved {len(recent_games) if recent_games else 0} recent games for player {player_id}")
            
            # Ensure recent_games is a list and has proper structure
            if not recent_games:
                recent_games = []
            
            # Log the structure of the first game for debugging
            if recent_games and len(recent_games) > 0:
                logging.info(f"Sample recent game structure: {recent_games[0]}")
                
        except Exception as games_error:
            logging.warning(f"Could not fetch recent games for player {player_id}: {games_error}")
            recent_games = []
        
        # Get career stats if available
        career_stats = None
        try:
            # This would be implemented if career stats exist
            career_stats = {}
        except Exception as career_error:
            logging.warning(f"Could not fetch career stats for player {player_id}: {career_error}")
        
        return jsonify({
            'success': True,
            'data': {
                'player': player,
                'season_stats': season_stats,
                'recent_games': recent_games,
                'career_stats': career_stats
            }
        })
        
    except Exception as e:
        logging.error(f"API get player error for player {player_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# This was tricky - had to handle cases where players have no shot data
# Added shot statistics calculation here instead of in frontend
# BUG: Sometimes shot_made field is null, need to handle that
@api_bp.route('/players/<int:player_id>/shot-chart')
def get_player_shot_chart_endpoint(player_id):
    """Get player shot chart data with filtering options """
    try:
        game_id = request.args.get('game_id', type=int)
        season = request.args.get('season', '2024-25')
        shot_type = request.args.get('shot_type', 'all')  # all, made, missed
        
        # Get shot data from Supabase
        shot_data = current_app.supabase.get_player_shot_chart(
            player_id, 
            game_id=game_id, 
            season=season
        )
        
        # Ensure shot_data is a list
        if not shot_data:
            shot_data = []
        
        # Filter by shot type if specified (this is done on frontend, but keeping for completeness)
        if shot_type == 'made':
            shot_data = [shot for shot in shot_data if shot.get('shot_made')]
        elif shot_type == 'missed':
            shot_data = [shot for shot in shot_data if not shot.get('shot_made')]
        
        # Add shot statistics
        total_shots = len(shot_data)
        made_shots = sum(1 for shot in shot_data if shot.get('shot_made'))
        shot_percentage = (made_shots / total_shots * 100) if total_shots > 0 else 0
        
        return jsonify({
            'success': True,
            'data': shot_data,
            'stats': {
                'total_shots': total_shots,
                'made_shots': made_shots,
                'missed_shots': total_shots - made_shots,
                'shot_percentage': round(shot_percentage, 1)
            }
        })
        
    except Exception as e:
        logging.error(f"API get shot chart error for player {player_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/teams')
def get_teams():
    """Get all teams with enhanced filtering"""
    try:
        conference = request.args.get('conference', '')  # eastern, western
        division = request.args.get('division', '')
        
        teams = current_app.supabase.get_all_teams()
        
        # Filter by conference 
        if conference:
            conference_filter = conference.lower()
            teams = [
                team for team in teams 
                if team.get('conference', '').lower().startswith(conference_filter)
            ]
        
        # Filter by division 
        if division:
            teams = [
                team for team in teams 
                if team.get('division', '').lower() == division.lower()
            ]
        
        # Add team records with detailed stats
        for team in teams:
            try:
                # Get basic record
                team_record = current_app.supabase.get_team_record(team['id'])
                team.update(team_record)
                
                # Calculate L10 and streak from recent games
                recent_games = current_app.supabase.get_team_recent_games(team['id'], limit=20)
                l10_and_streak = calculate_team_l10_and_streak(team['id'], recent_games)
                team.update(l10_and_streak)
                
            except Exception as record_error:
                logging.warning(f"Could not fetch record for team {team['id']}: {record_error}")
                team.update({
                    'wins': 0,
                    'losses': 0,
                    'win_percentage': 0.0,
                    'last_ten': '0-0',
                    'streak': '-'
                })
        
        return jsonify({
            'success': True,
            'data': teams
        })
        
    except Exception as e:
        logging.error(f"API get teams error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def calculate_team_l10_and_streak(team_id, recent_games):
    """Calculate last 10 games record and current streak"""
    try:
        if not recent_games:
            return {'last_ten': '0-0', 'streak': '-'}
        
        # Sort games by date (most recent first)
        games = sorted(recent_games, key=lambda g: g.get('game_date', ''), reverse=True)
        
        l10_results = []
        streak_count = 0
        streak_type = None
        
        for i, game in enumerate(games):
            if not game.get('home_score') or not game.get('away_score'):
                continue
                
            # Determine if team won
            is_home = game.get('home_team_id') == team_id
            team_score = game.get('home_score') if is_home else game.get('away_score')
            opponent_score = game.get('away_score') if is_home else game.get('home_score')
            
            won = team_score > opponent_score
            
            # Calculate last 10
            if len(l10_results) < 10:
                l10_results.append('W' if won else 'L')
            
            # Calculate streak (from most recent game)
            if i == 0:
                streak_type = 'W' if won else 'L'
                streak_count = 1
            elif streak_type and ((won and streak_type == 'W') or (not won and streak_type == 'L')):
                streak_count += 1
            else:
                break  # Streak broken
        
        # Format last 10
        l10_wins = l10_results.count('W')
        l10_losses = len(l10_results) - l10_wins
        last_ten = f"{l10_wins}-{l10_losses}"
        
        # Format streak
        streak = f"{streak_type}{streak_count}" if streak_type and streak_count > 0 else '-'
        
        return {
            'last_ten': last_ten,
            'streak': streak
        }
        
    except Exception as e:
        logging.error(f"Error calculating L10 and streak for team {team_id}: {str(e)}")
        return {'last_ten': '0-0', 'streak': '-'}
        
@api_bp.route('/teams/<int:team_id>')
def get_team(team_id):
    """Get team details """
    try:
        team = current_app.supabase.get_team_by_id(team_id)
        
        if not team:
            return jsonify({
                'success': False,
                'error': 'Team not found'
            }), 404
        
        # Get additional data
        roster = current_app.supabase.get_team_roster(team_id)
        recent_games = current_app.supabase.get_team_recent_games(team_id, limit=10)
        team_stats = current_app.supabase.get_team_season_stats(team_id)
        
        # Calculate additional team metrics
        roster_stats = calculate_roster_averages(roster)
        
        return jsonify({
            'success': True,
            'data': {
                'team': team,
                'roster': roster,
                'recent_games': recent_games,
                'team_stats': team_stats,
                'roster_stats': roster_stats
            }
        })
        
    except Exception as e:
        logging.error(f"API get team error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api_bp.route('/teams/<int:team_id>/roster')
def get_team_roster(team_id):
    """Get team roster"""
    try:
        roster = current_app.supabase.get_team_roster(team_id)
        
        return jsonify({
            'success': True,
            'data': roster
        })
        
    except Exception as e:
        logging.error(f"API get roster error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/games')
def get_games():
    """Get recent games with enhanced filtering"""
    try:
        limit = min(request.args.get('limit', 10, type=int), 50)  # Max 50 games
        team_id = request.args.get('team_id', type=int)
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        
        games = current_app.supabase.get_recent_games(
            limit=limit,
            team_id=team_id,
            date_from=date_from,
            date_to=date_to
        )
        
        return jsonify({
            'success': True,
            'data': games
        })
        
    except Exception as e:
        logging.error(f"API get games error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api_bp.route('/games/<int:game_id>')
def get_game(game_id):
    """Get detailed game information"""
    try:
        game = current_app.supabase.get_game_by_id(game_id)
        
        if not game:
            return jsonify({
                'success': False,
                'error': 'Game not found'
            }), 404
        
        # Get game stats if available
        game_stats = current_app.supabase.get_game_player_stats(game_id)
        
        return jsonify({
            'success': True,
            'data': {
                'game': game,
                'player_stats': game_stats
            }
        })
        
    except Exception as e:
        logging.error(f"API get game error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/search')
def search():
    """Enhanced global search endpoint"""
    try:
        query = request.args.get('q', '').strip()
        search_type = request.args.get('type', 'all')  # all, players, teams
        limit = min(request.args.get('limit', 10, type=int), 20)
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Search query is required'
            }), 400
        
        results = {'players': [], 'teams': []}
        
        if search_type in ['all', 'players']:
            # Search players with filtering
            player_results = current_app.supabase.get_players_paginated(
                page=1,
                per_page=limit,
                search=query
            )
            results['players'] = player_results['players']
        
        if search_type in ['all', 'teams']:
            # Search teams with better matching
            teams = current_app.supabase.get_all_teams()
            results['teams'] = [
                team for team in teams 
                if (query.lower() in team['name'].lower() or 
                    query.lower() in team['city'].lower() or 
                    query.lower() in team['abbreviation'].lower())
            ][:limit]
        
        return jsonify({
            'success': True,
            'data': results,
            'query': query,
            'total_results': len(results['players']) + len(results['teams'])
        })
        
    except Exception as e:
        logging.error(f"API search error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/rosters', methods=['GET', 'POST'])
@require_auth
def manage_rosters():
    """Enhanced roster management"""
    user = get_current_user()
    
    if request.method == 'GET':
        try:
            rosters = current_app.supabase.get_user_rosters(user['id'])
            
            # Add player count and roster stats to each roster
            for roster in rosters:
                try:
                    roster_players = current_app.supabase.get_roster_players(roster['id'])
                    roster['player_count'] = len(roster_players)
                    roster['roster_stats'] = calculate_roster_averages([rp.get('players', {}) for rp in roster_players])
                except Exception as roster_error:
                    logging.warning(f"Could not fetch roster data for {roster['id']}: {roster_error}")
                    roster['player_count'] = 0
                    roster['roster_stats'] = {}
            
            return jsonify({
                'success': True,
                'data': rosters
            })
            
        except Exception as e:
            logging.error(f"API get rosters error: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    # POST - Create new roster with validation
    try:
        data = request.get_json()
        
        if not data or not data.get('name'):
            return jsonify({
                'success': False,
                'error': 'Roster name is required'
            }), 400
        
        # Validate roster name length
        name = data['name'].strip()
        if len(name) < 2:
            return jsonify({
                'success': False,
                'error': 'Roster name must be at least 2 characters'
            }), 400
        
        if len(name) > 100:
            return jsonify({
                'success': False,
                'error': 'Roster name must be less than 100 characters'
            }), 400
        
        # Check for duplicate roster names for this user
        existing_rosters = current_app.supabase.get_user_rosters(user['id'])
        if any(roster['name'].lower() == name.lower() for roster in existing_rosters):
            return jsonify({
                'success': False,
                'error': 'You already have a roster with this name'
            }), 400
        
        result = current_app.supabase.create_roster(
            user_id=user['id'],
            name=name,
            description=data.get('description', '').strip()[:500],  # Limit description length
            is_public=data.get('is_public', False)
        )
        
        if result['success']:
            return jsonify({
                'success': True,
                'data': result['roster']
            })
        else:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 400
            
    except Exception as e:
        logging.error(f"API create roster error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api_bp.route('/rosters/<int:roster_id>')
@require_auth
def get_roster(roster_id):
    """Get roster details with enhanced stats"""
    try:
        user = get_current_user()
        roster = current_app.supabase.get_roster_by_id(roster_id)
        
        if not roster:
            return jsonify({
                'success': False,
                'error': 'Roster not found'
            }), 404
        
        # Check ownership or public access
        if roster['user_id'] != user['id'] and not roster.get('is_public'):
            return jsonify({
                'success': False,
                'error': 'Access denied'
            }), 403
        
        # Get roster players with enhanced data
        roster_players = current_app.supabase.get_roster_players(roster_id)
        
        # Calculate roster statistics
        roster_stats = calculate_roster_averages([rp.get('players', {}) for rp in roster_players])
        
        return jsonify({
            'success': True,
            'data': {
                'roster': roster,
                'players': roster_players,
                'stats': roster_stats
            }
        })
        
    except Exception as e:
        logging.error(f"API get roster error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api_bp.route('/rosters/<int:roster_id>/players', methods=['POST', 'DELETE'])
@require_auth
def manage_roster_players(roster_id):
    """Add or remove players from roster"""
    try:
        user = get_current_user()
        roster = current_app.supabase.get_roster_by_id(roster_id)
        
        if not roster or roster['user_id'] != user['id']:
            return jsonify({
                'success': False,
                'error': 'Roster not found or access denied'
            }), 403
        
        if request.method == 'POST':
            # Add player to roster
            data = request.get_json()
            player_id = data.get('player_id')
            position_slot = data.get('position_slot')
            
            if not player_id:
                return jsonify({
                    'success': False,
                    'error': 'Player ID is required'
                }), 400
            
            # Check roster size limit
            current_players = current_app.supabase.get_roster_players(roster_id)
            if len(current_players) >= current_app.config.get('MAX_ROSTER_SIZE', 15):
                return jsonify({
                    'success': False,
                    'error': 'Roster is full'
                }), 400
            
            result = current_app.supabase.add_player_to_roster(roster_id, player_id, position_slot)
            
            if result['success']:
                return jsonify({
                    'success': True,
                    'message': 'Player added to roster'
                })
            else:
                return jsonify({
                    'success': False,
                    'error': result['error']
                }), 400
        
        else:  # DELETE
            # Remove player from roster
            player_id = request.args.get('player_id', type=int)
            
            if not player_id:
                return jsonify({
                    'success': False,
                    'error': 'Player ID is required'
                }), 400
            
            result = current_app.supabase.remove_player_from_roster(roster_id, player_id)
            
            if result['success']:
                return jsonify({
                    'success': True,
                    'message': 'Player removed from roster'
                })
            else:
                return jsonify({
                    'success': False,
                    'error': result['error']
                }), 400
        
    except Exception as e:
        logging.error(f"API manage roster players error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Only admin users can access these endpoints
# Added extra security checks because this syncs live NBA data
# TODO: Add logging for all admin actions
# Enhanced Admin API endpoints
@api_bp.route('/admin/sync', methods=['POST'])
@require_role('admin')
def admin_sync_data():
    """Enhanced admin sync with better options"""
    try:
        data = request.get_json()
        sync_type = data.get('sync_type', 'all')
        
        if sync_type == 'teams':
            result = current_app.nba_service.sync_teams()
        elif sync_type == 'players':
            team_id = data.get('team_id')
            result = current_app.nba_service.sync_players(team_id)
        elif sync_type == 'games':
            days_back = data.get('days_back', 30)
            max_games = data.get('max_games', 200)
            result = current_app.nba_service.sync_recent_games_enhanced(days_back, max_games)
        elif sync_type == 'player_stats':
            max_players = data.get('max_players', 100)
            result = current_app.nba_service.sync_player_stats_enhanced(max_players=max_players)
        elif sync_type == 'shot_charts':
            player_ids = data.get('player_ids', [])
            season = data.get('season', '2024-25')
            max_shots = data.get('max_shots', 1000)
            if player_ids:
                results = []
                for player_id in player_ids:
                    player_result = current_app.nba_service.sync_shot_chart_data_enhanced(
                        player_id, season, max_shots
                    )
                    results.append(player_result)
                result = {
                    'success': all(r['success'] for r in results),
                    'synced_count': sum(r.get('synced_count', 0) for r in results),
                    'results': results
                }
            else:
                return jsonify({
                    'success': False,
                    'error': 'Player IDs required for shot chart sync'
                }), 400
        else:
            result = current_app.nba_service.sync_all_data_enhanced()
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logging.error(f"API admin sync error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api_bp.route('/admin/stats')
@require_role('admin')
def admin_stats():
    """Get app statistics (admin only)"""
    try:
        stats = {}
        
        # Count teams
        teams_response = (
            current_app.supabase.client
                .schema("hoops")
                .table("teams")
                .select("id", count="exact")
                .execute()
        )
        stats['teams_count'] = teams_response.count or 0
        
        # Count players
        players_response = (
            current_app.supabase.client
                .schema("hoops")
                .table("players")
                .select("id", count="exact")
                .eq("is_active", True)
                .execute()
        )
        stats['players_count'] = players_response.count or 0
        
        # Count users
        users_response = (
            current_app.supabase.client
                .schema("hoops")
                .table("user_profiles")
                .select("id", count="exact")
                .execute()
        )
        stats['users_count'] = users_response.count or 0
        
        # Count games
        games_response = (
            current_app.supabase.client
                .schema("hoops")
                .table("games")
                .select("id", count="exact")
                .execute()
        )
        stats['games_count'] = games_response.count or 0
        
        # Count rosters
        rosters_response = (
            current_app.supabase.client
                .schema("hoops")
                .table("user_rosters")
                .select("id", count="exact")
                .execute()
        )
        stats['rosters_count'] = rosters_response.count or 0
        
        # Get last sync
        last_sync = current_app.supabase.get_last_sync_log()
        stats['last_sync'] = last_sync
        
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        logging.error(f"API admin stats error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Utility functions
def calculate_roster_averages(players):
    """Calculate average stats for a roster"""
    if not players:
        return {
            'avg_points': 0.0,
            'avg_rebounds': 0.0,
            'avg_assists': 0.0,
            'total_players': 0,
            'players_with_stats': 0
        }
    
    total_points = 0
    total_rebounds = 0
    total_assists = 0
    players_with_stats = 0
    
    for player in players:
        if not player:
            continue
            
        points = float(player.get('avg_points', 0) or 0)
        rebounds = float(player.get('avg_rebounds', 0) or 0)
        assists = float(player.get('avg_assists', 0) or 0)
        
        total_points += points
        total_rebounds += rebounds
        total_assists += assists
        
        if points > 0 or rebounds > 0 or assists > 0:
            players_with_stats += 1
    
    total_players = len([p for p in players if p])
    
    return {
        'avg_points': round(total_points / total_players, 1) if total_players > 0 else 0.0,
        'avg_rebounds': round(total_rebounds / total_players, 1) if total_players > 0 else 0.0,
        'avg_assists': round(total_assists / total_players, 1) if total_players > 0 else 0.0,
        'total_players': total_players,
        'players_with_stats': players_with_stats
    }

# Health check endpoint
@api_bp.route('/health')
def health_check():
    """Enhanced health check endpoint"""
    try:
        # Test database connection
        current_app.supabase.client.table("teams").select("id").limit(1).execute()
        
        # Get basic stats
        stats = {}
        try:
            teams_resp = current_app.supabase.client.schema("hoops").from_("teams").select("id", count="exact").execute()
            stats['teams_count'] = teams_resp.count or 0
            
            players_resp = current_app.supabase.client.schema("hoops").from_("players").select("id", count="exact").eq("is_active", True).execute()
            stats['active_players_count'] = players_resp.count or 0
        except:
            stats = {'error': 'Could not fetch stats'}
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'timestamp': 'now()',
            'stats': stats
        })
        
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e)
        }), 503


# roster edit, delete , share  route - specific        
@api_bp.route('/rosters/<int:roster_id>', methods=['PATCH'])
@require_auth
def update_roster(roster_id):
    """Update roster details"""
    try:
        user = get_current_user()
        roster = current_app.supabase.get_roster_by_id(roster_id)
        
        if not roster or roster['user_id'] != user['id']:
            return jsonify({'success': False, 'error': 'Roster not found or access denied'}), 403
        
        data = request.get_json()
        
        # Validate data
        name = data.get('name', '').strip()
        if not name:
            return jsonify({'success': False, 'error': 'Roster name is required'}), 400
        
        # Update roster
        update_data = {
            'name': name[:100],  # Limit length
            'description': data.get('description', '')[:500],  # Limit length
            'is_public': data.get('is_public', False)
        }
        
        response = (
            current_app.supabase.client
                .schema("hoops")
                .from_("user_rosters")
                .update(update_data)
                .eq("id", roster_id)
                .execute()
        )
        
        return jsonify({'success': True, 'message': 'Roster updated successfully'})
        
    except Exception as e:
        logging.error(f"Update roster error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/rosters/<int:roster_id>', methods=['DELETE'])
@require_auth
def delete_roster(roster_id):
    """Delete a roster"""
    try:
        user = get_current_user()
        roster = current_app.supabase.get_roster_by_id(roster_id)
        
        if not roster or roster['user_id'] != user['id']:
            return jsonify({'success': False, 'error': 'Roster not found or access denied'}), 403
        
        # Delete roster players first
        current_app.supabase.client\
            .schema("hoops")\
            .from_("roster_players")\
            .delete()\
            .eq("roster_id", roster_id)\
            .execute()
        
        # Delete roster
        current_app.supabase.client\
            .schema("hoops")\
            .from_("user_rosters")\
            .delete()\
            .eq("id", roster_id)\
            .execute()
        
        return jsonify({'success': True, 'message': 'Roster deleted successfully'})
        
    except Exception as e:
        logging.error(f"Delete roster error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500      

# Had to clear cache immediately when favorites are updated
# Otherwise dashboard doesn't show changes right away
# NOTE: This could be optimized but works for now

@api_bp.route('/favorites', methods=['GET', 'POST', 'DELETE'])
@require_auth
def manage_favorites():
    """ manage user favorites with proper cache clearing and error handling"""
    try:
        user = get_current_user()
        logging.info(f"Favorites request - Method: {request.method}, User: {user['id']}")
        
        if request.method == 'POST':
            data = request.get_json()
            logging.info(f"Favorites POST data received: {data}")
            
            entity_type = data.get('entity_type') if data else None
            entity_id = data.get('entity_id') if data else None
            
            logging.info(f"Parsed - entity_type: {entity_type}, entity_id: {entity_id}")
            
            if not entity_type or not entity_id:
                logging.error(f"Missing data - entity_type: {entity_type}, entity_id: {entity_id}")
                return jsonify({'success': False, 'error': 'Missing entity_type or entity_id'}), 400
            
            # Check if already exists first to avoid duplicate error
            try:
                # Clear cache first to get fresh data
                current_app.supabase.cache.clear(f"user_favorites_{user['id']}")
                existing_favorites = current_app.supabase.get_user_favorites(user['id'])
                
                for fav in existing_favorites:
                    if fav.get('entity_type') == entity_type and fav.get('entity_id') == entity_id:
                        logging.info(f"Already in favorites - returning 400")
                        return jsonify({'success': False, 'error': 'Already in favorites'}), 400
                
                # Try to add the favorite
                logging.info(f"Calling add_favorite with user_id: {user['id']}, entity_type: {entity_type}, entity_id: {entity_id}")
                result = current_app.supabase.add_favorite(user['id'], entity_type, entity_id)
                logging.info(f"Add favorite result: {result}")
                
                if result.get('success'):
                    # Clear all user-related caches immediately
                    _clear_user_caches(user['id'])
                    return jsonify(result)
                else:
                    logging.error(f"Add favorite failed: {result}")
                    return jsonify(result), 400
                    
            except Exception as inner_e:
                logging.error(f"Inner exception in favorites: {str(inner_e)}")
                return jsonify({'success': False, 'error': f'Inner error: {str(inner_e)}'}), 500
            
        elif request.method == 'GET':
            # Always fetch fresh favorites for GET requests to ensure dashboard shows latest
            current_app.supabase.cache.clear(f"user_favorites_{user['id']}")
            favorites = current_app.supabase.get_user_favorites(user['id'])
            return jsonify({
                'success': True, 
                'data': favorites or []
            })
            
        elif request.method == 'DELETE':
            entity_type = request.args.get('entity_type')
            entity_id = request.args.get('entity_id')
            
            if not entity_type or not entity_id:
                return jsonify({'success': False, 'error': 'Missing entity_type or entity_id'}), 400
            
            result = current_app.supabase.remove_favorite(user['id'], entity_type, int(entity_id))
            
            if result.get('success'):
                # Clear all user-related caches immediately
                _clear_user_caches(user['id'])
            
            return jsonify(result)
            
    except Exception as e:
        logging.error(f"Favorites error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

def _clear_user_caches(user_id: str):
    """Clear all caches related to a user's data"""
    try:
        from flask import session
        
        # Clear Supabase caches
        current_app.supabase.cache.clear(f"user_favorites_{user_id}")
        current_app.supabase.cache.clear(f"user_rosters_{user_id}")
        current_app.supabase.cache.clear("dashboard_recent_games")  # Clear shared dashboard cache too
        
        # Clear session caches related to user data
        cache_keys_to_clear = []
        for key in list(session.keys()):
            if (key.startswith('cache_') and 
                ('favorites' in key or 'dashboard' in key or f'user_{user_id}' in key)):
                cache_keys_to_clear.append(key)
        
        for key in cache_keys_to_clear:
            session.pop(key, None)
        
        logging.info(f"Cleared {len(cache_keys_to_clear)} cache entries for user {user_id}")
        
    except Exception as e:
        logging.error(f"Error clearing user caches: {str(e)}")

# this new endpoint  clears dashboard cache
@api_bp.route('/clear-dashboard-cache', methods=['POST'])
@require_auth
def clear_dashboard_cache():
    """Clear dashboard cache for current user"""
    try:
        user = get_current_user()
        _clear_user_caches(user['id'])
        return jsonify({'success': True, 'message': 'Dashboard cache cleared'})
    except Exception as e:
        logging.error(f"Clear dashboard cache error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

        
@api_bp.route('/debug/teams')
def debug_teams():
    """Debug endpoint to see team data structure"""
    try:
        teams = current_app.supabase.get_all_teams()
        sample_team = teams[0] if teams else None
        
        if sample_team:
            # Try to get record for first team
            try:
                record = current_app.supabase.get_team_record(sample_team['id'])
                sample_team.update(record)
            except Exception as e:
                sample_team['record_error'] = str(e)
        
        return jsonify({
            'success': True,
            'total_teams': len(teams),
            'sample_team': sample_team,
            'all_team_names': [t.get('name', 'Unknown') for t in teams[:5]]
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
        
# DEBUGGING   debug data structure issues
@api_bp.route('/debug/player/<int:player_id>')
def debug_player_data(player_id):
    """Debug endpoint to check player data structure"""
    try:
        debug_info = {}
        
        # Get player info
        try:
            player = current_app.supabase.get_player_by_id(player_id)
            debug_info['player'] = {
                'found': player is not None,
                'data': player if player else None
            }
        except Exception as e:
            debug_info['player'] = {'error': str(e)}
        
        # Get season stats
        try:
            season_stats = current_app.supabase.get_player_season_stats(player_id)
            debug_info['season_stats'] = {
                'found': season_stats is not None,
                'data': season_stats if season_stats else None
            }
        except Exception as e:
            debug_info['season_stats'] = {'error': str(e)}
        
        # Get recent games
        try:
            recent_games = current_app.supabase.get_player_recent_games(player_id, limit=3)
            debug_info['recent_games'] = {
                'found': recent_games is not None,
                'count': len(recent_games) if recent_games else 0,
                'sample': recent_games[0] if recent_games and len(recent_games) > 0 else None
            }
        except Exception as e:
            debug_info['recent_games'] = {'error': str(e)}
        
        # Get shot chart data
        try:
            shot_data = current_app.supabase.get_player_shot_chart(player_id)
            debug_info['shot_chart'] = {
                'found': shot_data is not None,
                'count': len(shot_data) if shot_data else 0,
                'sample': shot_data[0] if shot_data and len(shot_data) > 0 else None
            }
        except Exception as e:
            debug_info['shot_chart'] = {'error': str(e)}
        
        return jsonify({
            'success': True,
            'player_id': player_id,
            'debug_info': debug_info
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
        
@api_bp.route('/admin/player-ids')
@require_auth
def get_all_player_ids():
    """Get all active player IDs for admin use"""
    try:
        user = get_current_user()
        if user.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        
        response = (
            current_app.supabase.client
                .schema("hoops")
                .from_("players")
                .select("id, nba_player_id, first_name, last_name")
                .eq("is_active", True)
                .order("last_name")
                .execute()
        )
        
        players = response.data or []
        
        # Just IDs comma-separated
        player_ids = [str(p['id']) for p in players]
        ids_string = ','.join(player_ids)
        
        # With names for reference
        players_with_names = [
            f"{p['id']} ({p['first_name']} {p['last_name']})" 
            for p in players
        ]
        
        return jsonify({
            'success': True,
            'total_players': len(players),
            'ids_only': ids_string,
            'players_with_names': players_with_names[:50],  # First 50 for display
            'all_ids': player_ids
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500