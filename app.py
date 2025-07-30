# Main Flask application for Hoops Tracker
# Started simple but got pretty complex with all the caching
# TODO: Break this into smaller modules if it gets much bigger
import os
import logging
from flask import Flask, render_template, session, request, jsonify, redirect, url_for, flash, g
from flask_cors import CORS
from flask_session import Session
import tempfile
from datetime import datetime, timedelta, timezone
import threading
from dotenv import load_dotenv

# Import our optimized modules
from config import Config
from supabase_client import SupabaseClient
from nba_service import NBAService
try:
    from parallel_sync import ParallelSyncService
except ImportError:
    class ParallelSyncService:
        def __init__(self, *args, **kwargs):
            pass
from auth import auth_bp, require_auth, get_current_user
from api import api_bp

# Load environment variables
load_dotenv()

# Global sync state management with thread safety
sync_status = {
    "active": False, 
    "type": None, 
    "stopped": False,
    "start_time": None,
    "progress": 0,
    "message": ""
}
sync_lock = threading.Lock()
logger = logging.getLogger(__name__)


# Using app factory pattern to make testing easier
# Added sync_status to app context for background jobs
def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # IMPORTANT: Store sync_status in app for access from NBAService
    app.sync_status = sync_status
    
    # These helper functions generate NBA image URLs
    # Had issues with broken images so added fallbacks everywhere
    # NBA sometimes changes their URL patterns
    # helper functions for Jinja templates
    def get_player_headshot_url(nba_player_id, size='260x190'):
        """Generate NBA player headshot URL with fallbacks"""
        if not nba_player_id:
            return 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNDAiIGhlaWdodD0iNDAiIHZpZXdCb3g9IjAgMCA0MCA0MCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMjAiIGN5PSIyMCIgcj0iMTgiIGZpbGw9IiMzMzMzMzMiLz4KPGNpcmNsZSBjeD0iMjAiIGN5PSIxNSIgcj0iNSIgZmlsbD0id2hpdGUiLz4KPHBhdGggZD0iTTEwIDMwQzEwIDI1IDE0IDIwIDIwIDIwUzMwIDI1IDMwIDMwIiBmaWxsPSJ3aGl0ZSIvPgo8L3N2Zz4K'
        return f"https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/{size}/{nba_player_id}.png"

    def get_team_logo_url(nba_team_id, logo_type='primary'):
        """Generate NBA team logo URL with fallbacks"""
        if not nba_team_id:
            return 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNDAiIGhlaWdodD0iNDAiIHZpZXdCb3g9IjAgMCA0MCA0MCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3Qgd2lkdGg9IjQwIiBoZWlnaHQ9IjQwIiByeD0iNCIgZmlsbD0iIzMzMzMzMyIvPgo8dGV4dCB4PSIyMCIgeT0iMjIiIGZvbnQtZmFtaWx5PSJBcmlhbCIgZm9udC1zaXplPSIxMiIgZmlsbD0id2hpdGUiIHRleHQtYW5jaG9yPSJtaWRkbGUiPk5CQTwvdGV4dD4KPHN2Zz4K'
        
        logo_types = {
            'primary': 'primary/L/logo.svg',
            'global': 'global/L/logo.svg', 
            'secondary': 'secondary/L/logo.svg'
        }
        
        logo_path = logo_types.get(logo_type, logo_types['primary'])
        return f"https://cdn.nba.com/logos/nba/{nba_team_id}/{logo_path}"

    # Add helpers to Jinja environment
    app.jinja_env.globals.update(
        get_player_headshot_url=get_player_headshot_url,
        get_team_logo_url=get_team_logo_url
    )

    # Register enhanced custom Jinja filters
    def format_date(value, fmt='%b %d, %Y'):
        """date formatting with error handling"""
        try:
            if isinstance(value, str):
                value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if isinstance(value, datetime):
                return value.strftime(fmt)
            return value or 'N/A'
        except (ValueError, AttributeError):
            return 'N/A'
    
    def format_number(value, decimals=1):
        """Format numbers with proper handling of None/null values"""
        try:
            if value is None or value == '':
                return '0.0' if decimals > 0 else '0'
            return f"{float(value):.{decimals}f}"
        except (ValueError, TypeError):
            return '0.0' if decimals > 0 else '0'
    
    def format_percentage(value, decimals=1):
        """Format percentage values"""
        try:
            if value is None or value == '':
                return '0.0%'
            return f"{float(value) * 100:.{decimals}f}%"
        except (ValueError, TypeError):
            return '0.0%'
    
    app.jinja_env.filters.update({
        'date': format_date,
        'number': format_number,
        'percentage': format_percentage
    })
    
    #  session configuration
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['SESSION_FILE_DIR'] = tempfile.mkdtemp()
    app.config['SESSION_PERMANENT'] = False
    app.config['SESSION_USE_SIGNER'] = True
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    Session(app)
    
    # Enable CORS
    CORS(app, supports_credentials=True)
    
    #  logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # Initialize optimized services
    logger.info("Initializing optimized services...")
    supabase_client = SupabaseClient()
    nba_service = NBAService()
    nba_service.set_supabase_client(supabase_client)
    
    try:
        parallel_sync = ParallelSyncService(supabase_client, nba_service, max_workers=3)  # Reduced workers
        logger.info("Parallel sync service initialized")
    except Exception as e:
        logger.warning(f"Parallel sync service failed to initialize: {e}")
        parallel_sync = None
    
    # Store services in app context
    app.supabase = supabase_client
    app.nba_service = nba_service
    app.parallel_sync = parallel_sync
    
    # Optimized session cache helper functions
    def get_cached_data(cache_key, fetch_function, cache_duration_minutes=15):
        """Get data from session cache or fetch if expired with better error handling"""
        try:
            cache_data = session.get(f'cache_{cache_key}')
            
            if cache_data and isinstance(cache_data, dict) and 'timestamp' in cache_data:
                try:
                    cached_time = datetime.fromisoformat(cache_data['timestamp'].replace('Z', '+00:00'))
                    if datetime.now(timezone.utc) - cached_time < timedelta(minutes=cache_duration_minutes):        
                        logger.debug(f"Cache hit for {cache_key}")
                        return cache_data.get('data')
                except (ValueError, KeyError):
                    pass
            
            # Cache expired or doesn't exist, fetch new data
            logger.debug(f"Cache miss for {cache_key}, fetching fresh data")
            fresh_data = fetch_function()
            session[f'cache_{cache_key}'] = {
                'data': fresh_data,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            return fresh_data
            
        except Exception as e:
            logger.error(f"Cache error for {cache_key}: {str(e)}")
            try:
                return fetch_function()
            except Exception as fetch_error:
                logger.error(f"Fetch error for {cache_key}: {str(fetch_error)}")
                return None
    
    def get_cached_user_data(user_id, data_type, fetch_function, cache_duration_minutes=10):
        """Get user-specific cached data with error handling"""
        cache_key = f'user_{user_id}_{data_type}'
        return get_cached_data(cache_key, fetch_function, cache_duration_minutes)
    
    def clear_cache(cache_pattern=None):
        """cache clearing with pattern matching"""
        try:
            if cache_pattern:
                keys_to_remove = [k for k in session.keys() if k.startswith(f'cache_{cache_pattern}')]
            else:
                keys_to_remove = [k for k in session.keys() if k.startswith('cache_')]
            
            for key in keys_to_remove:
                session.pop(key, None)
                
            logger.info(f"Cleared {len(keys_to_remove)} session cache entries for pattern: {cache_pattern}")
            
            # Also clear Supabase client cache
            if hasattr(app.supabase, 'clear_cache'):
                app.supabase.clear_cache(cache_pattern)
            
            # Clear NBA service cache
            if hasattr(app.nba_service, 'clear_cache'):
                app.nba_service.clear_cache()
                
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
    
    def _clear_sync_related_caches(sync_type: str):
        """Clear caches related to the sync type"""
        try:
            if sync_type == 'teams':
                clear_cache('team')
            elif sync_type == 'players':
                clear_cache('player')
            elif sync_type == 'games':
                clear_cache('game')
                clear_cache('recent_games')
            elif sync_type == 'player_stats':
                clear_cache('player_stats')
                clear_cache('player_recent')
            elif sync_type == 'shot_charts':
                clear_cache('shot_chart')
                clear_cache('player_shot')
            elif sync_type in ['all', 'all_with_shots']:
                # Clear all caches for full sync
                clear_cache()
            else:
                # Clear all caches for unknown sync types
                clear_cache()
                
            logger.info(f"Cleared caches for {sync_type} sync")
        except Exception as e:
            logger.error(f"Error clearing sync caches: {str(e)}")
    
    
    # Register blueprints
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(api_bp, url_prefix='/api')

    # before request handlers
    @app.before_request
    def before_request():
        """request preprocessing with better error handling"""
        try:
            g.current_user = get_current_user()
            
            # Cache teams data globally with better error handling
            def fetch_teams():
                try:
                    teams = app.supabase.get_all_teams()
                    # Ensure conference data is properly formatted
                    for team in teams:
                        if team.get('conference'):
                            conf = team['conference'].lower()
                            if 'east' in conf:
                                team['conference'] = 'Eastern'
                            elif 'west' in conf:
                                team['conference'] = 'Western'
                    return teams
                except Exception as e:
                    logger.error(f"Error fetching teams: {str(e)}")
                    return []
            
            g.all_teams = get_cached_data('teams_global', fetch_teams, cache_duration_minutes=60)
            
            # Cleanup expired caches periodically
            if hasattr(app.supabase, 'cleanup_expired_cache'):
                app.supabase.cleanup_expired_cache()
            
        except Exception as e:
            logger.error(f"Before request error: {str(e)}")
            g.current_user = None
            g.all_teams = []

    # route handlers
    @app.route('/')
    def index():
        """home page with better data loading"""
        try:
            featured_data = {}
            
            if g.all_teams:
                featured_data['team_count'] = len(g.all_teams)
                featured_data['conferences'] = {
                    'Eastern': len([t for t in g.all_teams if t.get('conference') == 'Eastern']),
                    'Western': len([t for t in g.all_teams if t.get('conference') == 'Western'])
                }
            
            # Get recent games for homepage
            try:
                recent_games = get_cached_data(
                    'homepage_recent_games',
                    lambda: app.supabase.get_recent_games(limit=5),
                    cache_duration_minutes=10
                )
                featured_data['recent_games'] = recent_games[:3] if recent_games else []
            except Exception as e:
                logger.error(f"Error loading recent games for homepage: {str(e)}")
                featured_data['recent_games'] = []
            
            return render_template('index.html', featured_data=featured_data)
            
        except Exception as e:
            logger.error(f"Index page error: {str(e)}")
            return render_template('index.html', featured_data={})
    
    
    # Dashboard needs fresh favorites data to show updates immediately
    # Cache clearing here is important for user experience
    # BUG: Sometimes favorites count doesn't update, investigating...
    @app.route('/dashboard')
    @require_auth
    def dashboard():
        """dashboard with fresh favorites data"""
        try:
            user = g.current_user
            
            # Always fetch fresh favorites for dashboard to ensure immediate updates
            def fetch_fresh_favorites():
                try:
                    # Clear cache first to ensure fresh data
                    app.supabase.cache.clear(f"user_favorites_{user['id']}")
                    return app.supabase.get_user_favorites(user['id']) or []
                except Exception as e:
                    logger.error(f"Error fetching fresh favorites for user {user['id']}: {str(e)}")
                    return []
            
            # Get fresh favorites (no caching for dashboard to ensure immediate updates)
            favorites = fetch_fresh_favorites()
            
            # Cache recent games with shorter duration for dashboard
            recent_games = get_cached_data(
                'dashboard_recent_games',
                lambda: app.supabase.get_recent_games(limit=10) or [],
                cache_duration_minutes=5
            )
            
            # Log for debugging
            logger.info(f"Dashboard loaded for user {user['id']} with {len(favorites)} favorites")
            
            return render_template('dashboard.html',  
                                 user=user,  
                                 favorites=favorites or [],  
                                 recent_games=recent_games or [])
                                 
        except Exception as e:
            logger.error(f"Dashboard error: {str(e)}")
            flash('Error loading dashboard data', 'error')
            return render_template('dashboard.html', user=g.current_user, favorites=[], recent_games=[])
                
    @app.route('/players')
    def players():
        """players page with better filtering and error handling"""
        try:
            page = request.args.get('page', 1, type=int)
            per_page = min(request.args.get('per_page', 20, type=int), 50)
            search = request.args.get('search', '').strip()
            team_id = request.args.get('team_id', type=int)
            position = request.args.get('position', '').strip()
            
            # Create cache key based on search parameters
            cache_key = f'players_{page}_{per_page}_{search}_{team_id}_{position}'
            
            def fetch_players():
                try:
                    return app.supabase.get_players_paginated(
                        page=page,  
                        per_page=per_page,  
                        search=search,  
                        team_id=team_id,
                        position=position
                    )
                except Exception as e:
                    logger.error(f"Error fetching players: {str(e)}")
                    return {
                        'players': [],
                        'pagination': {
                            'current_page': 1, 'total_pages': 1, 'total_count': 0,
                            'has_next': False, 'has_prev': False
                        }
                    }
            
            # Cache players data for 10 minutes
            players_data = get_cached_data(cache_key, fetch_players, cache_duration_minutes=10)
            
            # Get unique positions for filter
            positions = ['PG', 'SG', 'SF', 'PF', 'C', 'G', 'F']
            
            return render_template('players.html',  
                                 players=players_data.get('players', []),
                                 pagination=players_data.get('pagination', {}),
                                 teams=g.all_teams or [],
                                 positions=positions,
                                 search=search,
                                 selected_team=team_id,
                                 selected_position=position)
                                 
        except Exception as e:
            logger.error(f"Players page error: {str(e)}")
            flash('Error loading players data', 'error')
            return render_template('players.html', players=[], pagination={}, teams=[], positions=[])
    
    @app.route('/player/<int:player_id>')
    def player_detail(player_id):
        """player detail page with comprehensive data"""
        try:
            # Cache player data
            def fetch_player():
                player = app.supabase.get_player_by_id(player_id)
                if not player:
                    return None
                return player
            
            player = get_cached_data(
                f'player_{player_id}',
                fetch_player,
                cache_duration_minutes=60
            )
            
            if not player:
                flash('Player not found', 'error')
                return redirect(url_for('players'))
            
            # Cache season stats with multiple seasons fallback
            def fetch_season_stats():
                seasons_to_try = ['2024-25', '2023-24', '2022-23']
                for season in seasons_to_try:
                    stats = app.supabase.get_player_season_stats(player_id, season)
                    if stats and stats.get('games_played', 0) > 0:
                        return stats
                return app.supabase.get_player_season_stats(player_id)
            
            season_stats = get_cached_data(
                f'player_stats_{player_id}',
                fetch_season_stats,
                cache_duration_minutes=30
            )
            
            # Cache recent games
            recent_games = get_cached_data(
                f'player_recent_{player_id}',
                lambda: app.supabase.get_player_recent_games(player_id, limit=10) or [],
                cache_duration_minutes=15
            )
            
            # Cache shot chart data
            shot_chart_data = get_cached_data(
                f'shot_chart_{player_id}',
                lambda: app.supabase.get_player_shot_chart(player_id) or [],
                cache_duration_minutes=120
            )
            
            return render_template('player_detail.html',
                                 player=player,
                                 season_stats=season_stats or {},
                                 recent_games=recent_games or [],
                                 shot_chart_data=shot_chart_data or [])
                                 
        except Exception as e:
            logger.error(f"Player detail error for player {player_id}: {str(e)}")
            flash('Error loading player data', 'error')
            return redirect(url_for('players'))
            
    
    
    @app.route('/teams')
    def teams():
        """teams page with better conference handling"""
        try:
            teams = g.all_teams or []
            
            # Ensure all teams have proper conference data
            eastern_teams = [t for t in teams if t.get('conference') == 'Eastern']
            western_teams = [t for t in teams if t.get('conference') == 'Western']
            
            # Log warning if teams don't have conference data
            if len(eastern_teams) + len(western_teams) < len(teams):
                unknown_conference_count = len(teams) - len(eastern_teams) - len(western_teams)
                logger.warning(f"{unknown_conference_count} teams missing conference data")
                
                # Try to assign based on team names/cities if possible
                from nba_service import Config
                for team in teams:
                    if not team.get('conference') or team['conference'] not in ['Eastern', 'Western']:
                        fallback_conf = Config.get_team_conference(team.get('name', ''))
                        if fallback_conf:
                            team['conference'] = fallback_conf
                        else:
                            team['conference'] = 'Eastern'
            
            return render_template('teams.html', teams=teams)
            
        except Exception as e:
            logger.error(f"Teams page error: {str(e)}")
            flash('Error loading teams data', 'error')
            return render_template('teams.html', teams=[])
    
    @app.route('/team/<int:team_id>')
    def team_detail(team_id):
        """team detail page"""
        try:
            # Cache team data
            team = get_cached_data(
                f'team_{team_id}',
                lambda: app.supabase.get_team_by_id(team_id),
                cache_duration_minutes=60
            )
            
            if not team:
                flash('Team not found', 'error')
                return redirect(url_for('teams'))
            
            # Cache roster with error handling
            def fetch_roster():
                try:
                    roster = app.supabase.get_team_roster(team_id)
                    if roster:
                        roster.sort(key=lambda p: p.get('jersey_number') or 99)
                    return roster or []
                except Exception as e:
                    logger.error(f"Error fetching roster for team {team_id}: {str(e)}")
                    return []
            
            roster = get_cached_data(
                f'team_roster_{team_id}',
                fetch_roster,
                cache_duration_minutes=30
            )
            
            # Cache recent games
            recent_games = get_cached_data(
                f'team_games_{team_id}',
                lambda: app.supabase.get_team_recent_games(team_id, limit=10) or [],
                cache_duration_minutes=15
            )
            
            # Cache team stats
            team_stats = get_cached_data(
                f'team_stats_{team_id}',
                lambda: app.supabase.get_team_season_stats(team_id) or {},
                cache_duration_minutes=30
            )
            
            return render_template('team_detail.html',
                                 team=team,
                                 roster=roster,
                                 recent_games=recent_games,
                                 team_stats=team_stats)
                                 
        except Exception as e:
            logger.error(f"Team detail error for team {team_id}: {str(e)}")
            flash('Error loading team data', 'error')
            return redirect(url_for('teams'))
    
    @app.route('/rosters', methods=['GET', 'POST'])
    @require_auth
    def rosters():
        """rosters page"""
        user = g.current_user
        
        if request.method == 'POST':
            # Handle roster creation with enhanced validation
            try:
                data = request.get_json()
                name = data.get('name', '').strip()
                description = data.get('description', '').strip()
                is_public = data.get('is_public', False)
                
                # validation
                if not name:
                    return jsonify({'success': False, 'error': 'Roster name is required'}), 400
                
                if len(name) < 2:
                    return jsonify({'success': False, 'error': 'Roster name must be at least 2 characters'}), 400
                
                if len(name) > 100:
                    return jsonify({'success': False, 'error': 'Roster name must be less than 100 characters'}), 400
                
                # Check for duplicate names
                existing_rosters = app.supabase.get_user_rosters(user['id'])
                if any(r['name'].lower() == name.lower() for r in existing_rosters):
                    return jsonify({'success': False, 'error': 'You already have a roster with this name'}), 400
                
                result = app.supabase.create_roster(
                    user_id=user['id'],
                    name=name,
                    description=description[:500],
                    is_public=is_public
                )
                
                if result['success']:
                    # Clear user roster cache
                    clear_cache(f'user_{user["id"]}_rosters')
                    return jsonify({'success': True, 'roster': result['roster']})
                else:
                    return jsonify({'success': False, 'error': result['error']}), 500
                    
            except Exception as e:
                logger.error(f"Error creating roster: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500
        
        # GET request - show rosters page
        try:
            def fetch_user_rosters():
                rosters = app.supabase.get_user_rosters(user['id'])
                
                # Add player count to each roster
                for roster in rosters:
                    try:
                        player_count_response = (
                            app.supabase.client
                                .schema("hoops")
                                .from_("roster_players")
                                .select("id", count="exact")
                                .eq("roster_id", roster['id'])
                                .execute()
                        )
                        roster['player_count'] = player_count_response.count or 0
                    except Exception as e:
                        logger.error(f"Error getting player count for roster {roster['id']}: {e}")
                        roster['player_count'] = 0
                
                return rosters
            
            user_rosters = get_cached_user_data(
                user['id'],
                'rosters',
                fetch_user_rosters,
                cache_duration_minutes=10
            )
            
            return render_template('rosters.html', rosters=user_rosters or [])
            
        except Exception as e:
            logger.error(f"Error loading rosters: {str(e)}")
            flash('Error loading rosters', 'error')
            return render_template('rosters.html', rosters=[])

    @app.route('/roster/<int:roster_id>')
    @require_auth
    def roster_detail(roster_id):
        """roster detail page"""
        try:
            user = g.current_user
            
            # Cache roster data
            roster = get_cached_data(
                f'roster_{roster_id}',
                lambda: app.supabase.get_roster_by_id(roster_id),
                cache_duration_minutes=15
            )
            
            if not roster or roster['user_id'] != user['id']:
                flash('Roster not found or access denied', 'error')
                return redirect(url_for('rosters'))
            
            # Get roster players with enhanced error handling
            def fetch_roster_players():
                try:
                    return app.supabase.get_roster_players(roster_id) or []
                except Exception as e:
                    logger.error(f"Error fetching roster players for {roster_id}: {str(e)}")
                    return []
            
            roster_players = fetch_roster_players()
            
            return render_template('roster_detail.html',  
                                 roster=roster,  
                                 roster_players=roster_players)
                                 
        except Exception as e:
            logger.error(f"Roster detail error for roster {roster_id}: {str(e)}")
            flash('Error loading roster', 'error')
            return redirect(url_for('rosters'))

    @app.route('/standings')
    @require_auth
    def standings():
        """standings page with dynamic calculation"""
        try:
            teams = g.all_teams
            
            if not teams:
                return render_template('standings.html', 
                                     east_teams=[], 
                                     west_teams=[], 
                                     team_records={})
            
            # Separate teams by conference
            east_teams = [t for t in teams if t.get('conference', '').lower() in ['east', 'eastern']]
            west_teams = [t for t in teams if t.get('conference', '').lower() in ['west', 'western']]
            
            # Handle teams without conference data
            no_conference_teams = [t for t in teams if t.get('conference', '').lower() not in ['east', 'west', 'eastern', 'western']]
            if no_conference_teams:
                logger.warning(f"Found {len(no_conference_teams)} teams without conference data")
                for i, team in enumerate(no_conference_teams):
                    if i % 2 == 0:
                        east_teams.append(team)
                    else:
                        west_teams.append(team)
            
            return render_template('standings.html',  
                                 east_teams=east_teams,  
                                 west_teams=west_teams,
                                 team_records={})
                                 
        except Exception as e:
            logger.error(f"Standings error: {str(e)}")
            try:
                teams = g.all_teams or []
                east_teams = [t for t in teams if t.get('conference', '').lower() in ['east', 'eastern']]
                west_teams = [t for t in teams if t.get('conference', '').lower() in ['west', 'western']]
                
                if not east_teams and not west_teams and teams:
                    for i, team in enumerate(teams):
                        if i < len(teams) // 2:
                            east_teams.append(team)
                        else:
                            west_teams.append(team)
                
                return render_template('standings.html',  
                                     east_teams=east_teams,  
                                     west_teams=west_teams,
                                     team_records={})
            except Exception as fallback_error:
                logger.error(f"Standings fallback error: {str(fallback_error)}")
                return render_template('standings.html',  
                                     east_teams=[],  
                                     west_teams=[],
                                     team_records={})
                                     
    # This endpoint handles both regular and parallel sync
    # Added stop functionality because syncs can take forever
    # IMPORTANT: Clear caches after sync or data won't update on frontend    
    # ——— Admin Dashboard ———
    @app.route('/admin')
    @require_auth
    def admin():
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                flash('Admin access required', 'error')
                return redirect(url_for('dashboard'))
            
            # Get stats for admin dashboard with caching
            def fetch_admin_stats():
                stats = {}
                try:
                    # Count teams
                    teams_resp = app.supabase.client.schema("hoops").from_("teams").select("id", count="exact").execute()
                    stats['teams_count'] = teams_resp.count or 0
                    
                    # Count active players
                    players_resp = app.supabase.client.schema("hoops").from_("players").select("id", count="exact").eq("is_active", True).execute()
                    stats['players_count'] = players_resp.count or 0
                    
                    # Count games
                    games_resp = app.supabase.client.schema("hoops").from_("games").select("id", count="exact").execute()
                    stats['games_count'] = games_resp.count or 0
                    
                    # Count users
                    users_resp = app.supabase.client.schema("hoops").from_("user_profiles").select("id", count="exact").execute()
                    stats['users_count'] = users_resp.count or 0
                    
                except Exception as e:
                    logger.error(f"Error getting admin stats: {str(e)}")
                    
                return stats
            
            stats = get_cached_data('admin_stats', fetch_admin_stats, cache_duration_minutes=5)
            
            # Get last sync log
            last_sync = None
            try:
                last_sync = app.supabase.get_last_sync_log()
            except Exception as e:
                logger.error(f"Error getting sync log: {str(e)}")
            
            # Get cache stats for debugging
            cache_stats = {}
            try:
                if hasattr(app.supabase, 'get_cache_stats'):
                    cache_stats['supabase'] = app.supabase.get_cache_stats()
                if hasattr(app.nba_service, 'get_cache_stats'):
                    cache_stats['nba_service'] = app.nba_service.get_cache_stats()
            except Exception as e:
                logger.error(f"Error getting cache stats: {str(e)}")
            
            return render_template('admin.html', 
                                 last_sync=last_sync,
                                 cache_stats=cache_stats,
                                 **stats)
        except Exception as e:
            logger.error(f"Admin page error: {str(e)}")
            flash('Error loading admin page', 'error')
            return redirect(url_for('dashboard'))

    

    @app.route('/admin/sync-data', methods=['POST'])
    @require_auth
    def sync_data():
        user = g.current_user
        if user.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        
        with sync_lock:
            # Check if sync already in progress
            if sync_status["active"]:
                return jsonify({'error': 'Sync already in progress'}), 400
            
            sync_type = request.json.get('sync_type', 'all')
            use_parallel = request.json.get('parallel', True)
            
            # Set sync status
            sync_status["active"] = True
            sync_status["type"] = sync_type
            sync_status["stopped"] = False
            sync_status["start_time"] = datetime.now(timezone.utc)
            sync_status["progress"] = 0
            sync_status["message"] = f"Starting {sync_type} sync..."

        try:
            logger.info(f"Starting {sync_type} sync (parallel: {use_parallel})")
            
            if use_parallel and app.parallel_sync:
                # Use parallel sync with new shot chart support
                if sync_type == 'teams':
                    job_id = app.parallel_sync.sync_teams_parallel()
                elif sync_type == 'players':
                    batch_size = request.json.get('batch_size', 2)
                    max_teams = request.json.get('max_teams', 5)
                    job_id = app.parallel_sync.sync_players_parallel(batch_size, max_teams)
                elif sync_type == 'player_stats':
                    player_ids = request.json.get('player_ids')
                    batch_size = request.json.get('batch_size', 5)
                    job_id = app.parallel_sync.sync_player_stats_parallel(player_ids, batch_size)
                elif sync_type == 'shot_charts':
                    player_ids = request.json.get('player_ids', [])
                    season = request.json.get('season', '2024-25')
                    if not player_ids:
                        # Get top players if no specific IDs provided
                        try:
                            response = (
                                app.supabase.client
                                    .schema("hoops")
                                    .from_("player_season_stats")
                                    .select("players:player_id(nba_player_id)")
                                    .gte("games_played", 5)
                                    .order("points_per_game", desc=True)
                                    .limit(10)
                                    .execute()
                            )
                            player_ids = [
                                p['players']['nba_player_id'] 
                                for p in response.data 
                                if p.get('players') and p['players'].get('nba_player_id')
                            ]
                        except Exception as e:
                            return jsonify({'error': f'Could not get player IDs: {str(e)}'}), 400
                    
                    if not player_ids:
                        return jsonify({'error': 'No player IDs available for shot chart sync'}), 400
                        
                    job_id = app.parallel_sync.sync_shot_charts_parallel(player_ids, season)
                elif sync_type == 'all_with_shots':
                    # Custom sync all with shot charts
                    max_players_for_shots = request.json.get('max_players_for_shots', 5)
                    job_id = app.parallel_sync.sync_all_parallel()  # This will need to be updated too
                else:
                    job_id = app.parallel_sync.sync_all_parallel()

                _clear_sync_related_caches(sync_type)

                return jsonify({
                    'success': True,
                    'message': f'Parallel {sync_type} sync started',
                    'job_id': job_id,
                    'parallel': True
                })
            else:
                # Regular sync with new shot chart support
                if sync_type == 'teams':
                    result = app.nba_service.sync_teams()
                elif sync_type == 'players':
                    result = app.nba_service.sync_players()
                elif sync_type == 'games':
                    result = app.nba_service.sync_recent_games_enhanced()
                elif sync_type == 'player_stats':
                    max_players = request.json.get('max_players', None)
                    result = app.nba_service.sync_player_stats_enhanced(max_players=max_players)
                elif sync_type == 'shot_charts':
                    player_ids = request.json.get('player_ids', [])
                    season = request.json.get('season', '2024-25')
                    max_shots = request.json.get('max_shots', 500)
                    
                    if not player_ids:
                        return jsonify({'error': 'Player IDs required for shot chart sync'}), 400
                    
                    # Sync shot charts for multiple players
                    total_synced = 0
                    results = []
                    
                    for player_id in player_ids:
                        try:
                            player_result = app.nba_service.sync_shot_chart_data_enhanced(
                                player_id, season, max_shots
                            )
                            results.append(player_result)
                            total_synced += player_result.get('synced_count', 0)
                            
                            # Check for stop signal
                            if sync_status["stopped"]:
                                break
                                
                        except Exception as e:
                            logger.error(f"Error syncing shots for player {player_id}: {str(e)}")
                            results.append({'success': False, 'error': str(e)})
                    
                    result = {
                        'success': all(r.get('success', False) for r in results),
                        'synced_count': total_synced,
                        'results': results
                    }
                elif sync_type == 'all_with_shots':
                    max_players_for_shots = request.json.get('max_players_for_shots', 5)
                    result = app.nba_service.sync_all_data_enhanced(
                        include_shot_charts=True,
                        max_players_for_shots=max_players_for_shots
                    )
                else:
                    result = app.nba_service.sync_all_data_enhanced()

                message = f'{sync_type} sync was stopped by admin' if sync_status["stopped"] else f'{sync_type} data synced successfully'
                result["stopped"] = sync_status.get("stopped", False)

                _clear_sync_related_caches(sync_type)

                return jsonify({
                    'success': True,
                    'message': message,
                    'result': result,
                    'parallel': False,
                    'stopped': sync_status.get("stopped", False)
                })
                
        except Exception as e:
            logger.error(f"Sync error: {str(e)}")
            return jsonify({'error': str(e)}), 500
        finally:
            # Reset sync status
            with sync_lock:
                sync_status["active"] = False
                sync_status["type"] = None
                sync_status["stopped"] = False
                sync_status["progress"] = 0
                sync_status["message"] = ""

    @app.route('/admin/stop-sync', methods=['POST'])
    @require_auth
    def stop_sync():
        """Stop ongoing sync operation"""
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                return jsonify({'error': 'Admin access required'}), 403
            
            with sync_lock:
                if not sync_status["active"]:
                    return jsonify({
                        'success': False,
                        'error': 'No sync operation in progress'
                    }), 400
                
                # Set the stop flag
                sync_status["stopped"] = True
                sync_status["message"] = "Stopping sync..."
            
            logger.info(f"Admin requested stop for {sync_status['type']} sync")
            
            return jsonify({
                'success': True,
                'message': f'{sync_status["type"]} sync stopped',
                'progress': sync_status["progress"]
            })
            
        except Exception as e:
            logger.error(f"Stop sync error: {str(e)}")
            return jsonify({'error': str(e)}), 500

    @app.route('/admin/clear-cache', methods=['POST'])
    @require_auth
    def clear_app_cache():
        """Clear application caches (admin only)"""
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                return jsonify({'error': 'Admin access required'}), 403
            
            cache_type = request.json.get('cache_type', 'all')
            clear_cache(cache_type if cache_type != 'all' else None)
            
            return jsonify({
                'success': True,
                'message': f'Cache cleared: {cache_type}'
            })
            
        except Exception as e:
            logger.error(f"Cache clear error: {str(e)}")
            return jsonify({'error': str(e)}), 500

    @app.route('/admin/refresh-stats')
    @require_auth
    def refresh_stats():
        user = g.current_user
        if user.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        
        try:
            # Clear admin stats cache and fetch fresh data
            clear_cache('admin_stats')
            
            stats = {}
            teams_resp = app.supabase.client.schema("hoops").from_("teams").select("id", count="exact").execute()
            stats['teams_count'] = teams_resp.count or 0
            
            players_resp = app.supabase.client.schema("hoops").from_("players").select("id", count="exact").eq("is_active", True).execute()
            stats['players_count'] = players_resp.count or 0
            
            games_resp = app.supabase.client.schema("hoops").from_("games").select("id", count="exact").execute()
            stats['games_count'] = games_resp.count or 0
            
            users_resp = app.supabase.client.schema("hoops").from_("user_profiles").select("id", count="exact").execute()
            stats['users_count'] = users_resp.count or 0
            
            return jsonify({
                'success': True,
                **stats
            })
            
        except Exception as e:
            logger.error(f"Refresh stats error: {str(e)}")
            return jsonify({'error': str(e)}), 500

    @app.route('/admin/sync-status')
    @require_auth
    def get_sync_status():
        """Get current sync status"""
        user = g.current_user
        if user.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        
        with sync_lock:
            status = sync_status.copy()
            if status.get("start_time"):
                status["start_time"] = status["start_time"].isoformat()
        
        return jsonify(status)

    # error handlers
    # Custom error pages look much better than Flask defaults
    # Added helpful links for 404s
    @app.errorhandler(404)
    def not_found(error):
        """404 handler"""
        return render_template('error.html',  
                             error_code=404,  
                             error_message="The page you're looking for doesn't exist."), 404

    @app.errorhandler(500)
    def internal_error(error):
        """500 handler"""
        logger.error(f"Internal server error: {str(error)}")
        return render_template('error.html',  
                             error_code=500,  
                             error_message="Something went wrong on our end. Please try again later."), 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        """Handle unexpected exceptions"""
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        return render_template('error.html',
                             error_code=500,
                             error_message="An unexpected error occurred."), 500
                             
    # Debugging endpoint
    @app.route('/test-sync/<int:player_id>') 
    def test_sync_player(player_id):
        """Test sync for one player"""
        try:
            result = app.nba_service.sync_player_stats_enhanced(player_id=player_id, max_players=1)
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)})

    return app
    

if __name__ == '__main__':
    app = create_app()
    
    # development server configuration
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    port = int(os.environ.get('PORT', 3000))
    host = os.environ.get('HOST', '0.0.0.0')
    
    logger.info(f"Starting Optimized NBA Hoops Tracker on {host}:{port} (debug={debug_mode})")
    
    app.run(
        debug=debug_mode, 
        host=host, 
        port=port,
        threaded=True,
        use_reloader=debug_mode
    )           