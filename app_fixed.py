# app_fixed.py - Main Flask Application with Better Error Handling
import os
from flask import Flask, render_template, session, request, jsonify, redirect, url_for, flash, g
from flask_cors import CORS
from flask_session import Session
import tempfile
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import json
import traceback

# Load environment variables FIRST
load_dotenv()

# Import our modules
from config import Config
from supabase_client import SupabaseClient
from nba_service import NBAService
from parallel_sync import ParallelSyncService
from auth import auth_bp, require_auth, get_current_user
from api import api_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Configure logging EARLY
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Register custom Jinja filter for dates
    def format_date(value, fmt='%b %d, %Y'):
        if isinstance(value, datetime):
            return value.strftime(fmt)
        return value
    app.jinja_env.filters['date'] = format_date
    
    # Configure session to use filesystem
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['SESSION_FILE_DIR'] = tempfile.mkdtemp()
    app.config['SESSION_PERMANENT'] = False
    app.config['SESSION_USE_SIGNER'] = True
    Session(app)
    
    # Enable CORS
    CORS(app)
    
    # Initialize services with error handling
    try:
        logger.info("Initializing Supabase client...")
        supabase_client = SupabaseClient()
        logger.info("✅ Supabase client initialized")
        
        logger.info("Initializing NBA service...")
        nba_service = NBAService()
        nba_service.set_supabase_client(supabase_client)
        logger.info("✅ NBA service initialized")
        
        logger.info("Initializing parallel sync service...")
        parallel_sync = ParallelSyncService(supabase_client, nba_service, max_workers=4)
        logger.info("✅ Parallel sync service initialized")
        
    except Exception as e:
        logger.error(f"❌ Service initialization failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise e
    
    # Store services in app context
    app.supabase = supabase_client
    app.nba_service = nba_service
    app.parallel_sync = parallel_sync
    
    # Session cache helper functions
    def get_cached_data(cache_key, fetch_function, cache_duration_minutes=15):
        """Get data from session cache or fetch if expired"""
        try:
            cache_data = session.get(f'cache_{cache_key}')
            
            if cache_data:
                cached_time = datetime.fromisoformat(cache_data['timestamp'])
                if datetime.utcnow() - cached_time < timedelta(minutes=cache_duration_minutes):
                    return cache_data['data']
            
            # Cache expired or doesn't exist, fetch new data
            fresh_data = fetch_function()
            session[f'cache_{cache_key}'] = {
                'data': fresh_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            return fresh_data
        except Exception as e:
            logger.error(f"Cache error for {cache_key}: {str(e)}")
            # Return empty data structure to prevent crashes
            return [] if 'list' in str(type(fetch_function)) else {}
    
    def invalidate_cache(cache_pattern=None):
        """Invalidate specific cache patterns or all caches"""
        try:
            if cache_pattern:
                keys_to_remove = [k for k in session.keys() if k.startswith(f'cache_{cache_pattern}')]
            else:
                keys_to_remove = [k for k in session.keys() if k.startswith('cache_')]
            
            for key in keys_to_remove:
                session.pop(key, None)
        except Exception as e:
            logger.error(f"Cache invalidation error: {str(e)}")
    
    # Register blueprints
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(api_bp, url_prefix='/api')

    # Before request handlers for optimization
    @app.before_request
    def before_request():
        try:
            # Store commonly needed data in g for the request
            g.current_user = get_current_user()
            
            # Cache teams data globally (changes infrequently)
            if 'teams_last_fetch' not in session or \
               datetime.utcnow() - datetime.fromisoformat(session['teams_last_fetch']) > timedelta(hours=1):
                g.all_teams = app.supabase.get_all_teams()
                session['cached_teams'] = g.all_teams
                session['teams_last_fetch'] = datetime.utcnow().isoformat()
            else:
                g.all_teams = session.get('cached_teams', [])
        except Exception as e:
            logger.error(f"Before request error: {str(e)}")
            g.current_user = None
            g.all_teams = []

    @app.route('/')
    def index():
        return render_template('index.html')
    
    @app.route('/dashboard')
    @require_auth
    def dashboard():
        try:
            user = g.current_user
            
            # Simple fallback data if cache fails
            favorites = []
            recent_games = []
            
            try:
                favorites = app.supabase.get_user_favorites(user['id'])
            except Exception as e:
                logger.error(f"Error getting favorites: {str(e)}")
            
            try:
                recent_games = app.supabase.get_recent_games(limit=10)
            except Exception as e:
                logger.error(f"Error getting recent games: {str(e)}")
            
            return render_template('dashboard.html',  
                                 user=user,  
                                 favorites=favorites,  
                                 recent_games=recent_games)
        except Exception as e:
            logger.error(f"Dashboard error: {str(e)}")
            flash('Error loading dashboard', 'error')
            return redirect(url_for('index'))
    
    # ——— Admin Dashboard ———
    @app.route('/admin')
    @require_auth
    def admin():
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                flash('Admin access required', 'error')
                return redirect(url_for('dashboard'))
            
            # Get last sync log with error handling
            last_sync = None
            try:
                last_sync = app.supabase.get_last_sync_log()
            except Exception as e:
                logger.error(f"Error getting sync log: {str(e)}")
            
            return render_template('admin.html', last_sync=last_sync)
        except Exception as e:
            logger.error(f"Admin page error: {str(e)}")
            flash('Error loading admin page', 'error')
            return redirect(url_for('dashboard'))

    @app.route('/admin/sync-data', methods=['POST'])
    @require_auth
    def sync_data():
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                return jsonify({'error': 'Admin access required'}), 403
            
            sync_type = request.json.get('sync_type', 'all')
            use_parallel = request.json.get('parallel', False)  # Default to False for safety

            logger.info(f"Starting sync: type={sync_type}, parallel={use_parallel}")

            if use_parallel:
                try:
                    if sync_type == 'teams':
                        job_id = app.parallel_sync.sync_teams_parallel()
                    elif sync_type == 'players':
                        batch_size = request.json.get('batch_size', 5)
                        job_id = app.parallel_sync.sync_players_parallel(batch_size)
                    elif sync_type == 'player_stats':
                        player_ids = request.json.get('player_ids')
                        batch_size = request.json.get('batch_size', 10)
                        job_id = app.parallel_sync.sync_player_stats_parallel(player_ids, batch_size)
                    elif sync_type == 'shot_charts':
                        player_ids = request.json.get('player_ids', [])
                        season = request.json.get('season', '2024-25')
                        if not player_ids:
                            return jsonify({'error': 'Player IDs required for shot chart sync'}), 400
                        job_id = app.parallel_sync.sync_shot_charts_parallel(player_ids, season)
                    else:
                        job_id = app.parallel_sync.sync_all_parallel()

                    # Invalidate relevant caches after sync starts
                    invalidate_cache()

                    return jsonify({
                        'success': True,
                        'message': f'Parallel {sync_type} sync started',
                        'job_id': job_id,
                        'parallel': True
                    })
                except Exception as e:
                    logger.error(f"Parallel sync error: {str(e)}")
                    logger.error(traceback.format_exc())
                    return jsonify({'error': f'Parallel sync failed: {str(e)}'}), 500
            else:
                # Sequential sync (like test_sync.py)
                try:
                    if sync_type == 'teams':
                        result = app.nba_service.sync_teams()
                    elif sync_type == 'players':
                        result = app.nba_service.sync_players()
                    elif sync_type == 'games':
                        result = app.nba_service.sync_recent_games()
                    elif sync_type == 'stats':
                        result = app.nba_service.sync_player_stats()
                    else:
                        result = app.nba_service.sync_all_data()

                    logger.info(f"Sequential sync result: {result}")

                    # Invalidate relevant caches after sync completes
                    invalidate_cache()

                    return jsonify({
                        'success': True,
                        'message': f'{sync_type} data synced successfully',
                        'result': result,
                        'parallel': False
                    })
                except Exception as e:
                    logger.error(f"Sequential sync error: {str(e)}")
                    logger.error(traceback.format_exc())
                    return jsonify({'error': f'Sequential sync failed: {str(e)}'}), 500
                    
        except Exception as e:
            logger.error(f"Sync data endpoint error: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({'error': f'Request processing failed: {str(e)}'}), 500

    # Simple test endpoint to verify services work
    @app.route('/admin/test-services', methods=['POST'])
    @require_auth
    def test_services():
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                return jsonify({'error': 'Admin access required'}), 403
            
            results = {}
            
            # Test Supabase connection
            try:
                teams = app.supabase.get_all_teams()
                results['supabase'] = {'success': True, 'teams_count': len(teams)}
            except Exception as e:
                results['supabase'] = {'success': False, 'error': str(e)}
            
            # Test NBA service
            try:
                # Test creating the service (without calling API)
                test_service = NBAService()
                test_service.set_supabase_client(app.supabase)
                results['nba_service'] = {'success': True, 'message': 'Service created successfully'}
            except Exception as e:
                results['nba_service'] = {'success': False, 'error': str(e)}
            
            return jsonify({
                'success': True,
                'results': results
            })
            
        except Exception as e:
            logger.error(f"Test services error: {str(e)}")
            return jsonify({'error': str(e)}), 500

    # Direct sync endpoint (like test_sync.py)
    @app.route('/admin/direct-sync', methods=['POST'])
    @require_auth
    def direct_sync():
        """Direct sync endpoint that mimics test_sync.py behavior"""
        try:
            user = g.current_user
            if user.get('role') != 'admin':
                return jsonify({'error': 'Admin access required'}), 403
            
            sync_method = request.json.get('sync_method', 'sync_teams')
            
            # Get the method from nba_service
            sync_function = getattr(app.nba_service, sync_method, None)
            if not sync_function:
                return jsonify({'error': f'No method {sync_method}()'}), 400
            
            logger.info(f"🔄 Starting direct {sync_method}()...")
            
            # Call the method directly
            result = sync_function()
            
            logger.info(f"✅ {sync_method}() returned: {result}")
            
            # Get count from database (like test_sync.py)
            table_map = {
                'sync_teams': 'teams',
                'sync_players': 'players',
                'sync_recent_games': 'games',
                'sync_player_stats': 'player_stats'
            }
            
            table_name = table_map.get(sync_method, 'teams')
            
            try:
                resp = (
                    app.supabase.client
                        .schema("hoops")
                        .from_(table_name)
                        .select("id", count="exact")
                        .execute()
                )
                count = resp.count or 0
                logger.info(f"🎯 hoops.{table_name} now contains exactly {count} rows")
            except Exception as e:
                logger.error(f"Error counting rows: {str(e)}")
                count = "unknown"
            
            return jsonify({
                'success': True,
                'message': f'Direct {sync_method} completed',
                'result': result,
                'table_count': count,
                'table_name': table_name
            })
            
        except Exception as e:
            logger.error(f"Direct sync error: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({'error': str(e)}), 500

    @app.errorhandler(404)
    def not_found(error):
        return render_template('error.html',  
                             error_code=404,  
                             error_message="Page not found"), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {str(error)}")
        return render_template('error.html',  
                             error_code=500,  
                             error_message="Internal server error"), 500

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=3000)