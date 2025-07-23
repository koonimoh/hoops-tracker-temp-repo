"""
Main application routes for pages and views.
"""

import os
from flask import (
    Blueprint, render_template, request, jsonify,
    redirect, url_for, current_app
)
from app.auth.decorators import login_required
from app.auth.auth_service import get_current_user, auth_service
from app.services.search_service import search_service
from app.services.stats_service import stats_service
from app.services.bets_service import betting_service
from app.db.supabase import supabase
from app.core.logging import logger

main_bp = Blueprint('main', __name__)

# --- DATA ROUTES ---

@main_bp.route('/')
def index():
    user = get_current_user()
    if user:
        return redirect(url_for('main.dashboard'))
    try:
        top_scorers    = stats_service.get_league_leaders('pts', limit=5)
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
    user = get_current_user()
    try:
        recent_bets = betting_service.get_user_bets(user['user_id'], limit=5)
        bet_stats   = betting_service.get_betting_statistics(user['user_id'])
        wl_res      = supabase.table('watchlists')\
                       .select('*, players(name, position, teams(name, abbreviation))')\
                       .eq('user_id', user['user_id'])\
                       .limit(10)\
                       .execute()
        watchlist   = wl_res.data or []
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
    try:
        search_query = request.args.get('search', '')
        page         = int(request.args.get('page', 1))
        per_page     = 20

        if search_query:
            players_data = search_service.unified_player_search(search_query, limit=per_page)
        else:
            offset = (page - 1) * per_page
            res    = supabase.table('players')\
                      .select('*, teams(name, abbreviation)')\
                      .eq('is_active', True)\
                      .range(offset, offset + per_page - 1)\
                      .execute()
            players_data = res.data or []

        return render_template('players.html',
                               players=players_data,
                               search_query=search_query,
                               page=page)
    except Exception as e:
        logger.error(f"Players page error: {e}")
        return render_template('players.html', players=[], error="Failed to load players")

@main_bp.route('/players/<player_id>')
def player_detail(player_id):
    try:
        p_res = supabase.table('players')\
                .select('*, teams(name, abbreviation, city)')\
                .eq('id', player_id)\
                .single()\
                .execute()
        if p_res.error or not p_res.data:
            return render_template('player_detail.html', error="Player not found")

        player       = p_res.data
        season_stats = stats_service.get_player_season_stats(player_id)
        rg_res       = supabase.table('player_stats')\
                       .select('game_date, stat_key, stat_value, opponent_team_id, teams(name)')\
                       .eq('player_id', player_id)\
                       .order('game_date', desc=True)\
                       .limit(50)\
                       .execute()

        games_by_date = {}
        for stat in (rg_res.data or []):
            date = stat['game_date']
            if date not in games_by_date:
                games_by_date[date] = {
                    'game_date': date,
                    'opponent' : stat.get('teams', {}).get('name', 'Unknown'),
                    'stats'    : {}
                }
            games_by_date[date]['stats'][stat['stat_key']] = stat['stat_value']

        recent_games = []
        for gd in games_by_date.values():
            s = gd['stats']
            gd['points']   = s.get('pts', 0)
            gd['rebounds'] = s.get('reb', 0)
            gd['assists']  = s.get('ast', 0)
            recent_games.append(gd)
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
    user = get_current_user()
    try:
        user_bets       = betting_service.get_user_bets(user['user_id'], limit=50)
        bet_stats       = betting_service.get_betting_statistics(user['user_id'])
        pop_res         = supabase.table('players')\
                           .select('*, teams(name, abbreviation)')\
                           .eq('is_active', True)\
                           .limit(20)\
                           .execute()
        popular_players = pop_res.data or []
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
    user = get_current_user()
    try:
        wl_res = supabase.table('watchlists')\
                  .select('*, players(*, teams(name, abbreviation)), user_profiles!inner(subscription_tier)')\
                  .eq('user_id', user['user_id'])\
                  .order('added_at', desc=True)\
                  .execute()
        data = wl_res.data or []
        return render_template('watchlist.html', watchlist_players=data)
    except Exception as e:
        logger.error(f"Watchlist page error: {e}")
        return render_template('watchlist.html', watchlist_players=[])

@main_bp.route('/standings')
def standings():
    try:
        res       = supabase.table('team_standings')\
                    .select('*, teams(name, abbreviation, conference), seasons!inner(is_current)')\
                    .eq('seasons.is_current', True)\
                    .execute()
        standings = res.data or []
        east      = [s for s in standings if s['teams']['conference']=='East']
        west      = [s for s in standings if s['teams']['conference']=='West']
        east.sort(key=lambda x: x['pct'], reverse=True)
        west.sort(key=lambda x: x['pct'], reverse=True)
        return render_template('standings.html',
                               eastern_standings=east,
                               western_standings=west)
    except Exception as e:
        logger.error(f"Standings page error: {e}")
        return render_template('standings.html',
                               eastern_standings=[],
                               western_standings=[])

# --- AUTH ROUTES ---

@main_bp.route('/login', methods=['GET', 'POST'])
def login():
    """User login page."""
    if request.method == 'POST':
        res = auth_service.login_user(
            request.form['email'],
            request.form['password']
        )
        if res.get('success'):
            return redirect(url_for('main.dashboard'))
        return render_template('login.html', error=res.get('error'))
    return render_template('login.html')

@main_bp.route('/register', methods=['GET', 'POST'])
def register():
    """User registration page."""
    if request.method == 'POST':
        form = request.form
        res = auth_service.register_user(
            email=form['email'],
            password=form['password'],
            full_name=form.get('full_name')
        )
        if res.get('success'):
            return redirect(url_for('main.login'))
        return render_template('register.html', error=res.get('error'))
    return render_template('register.html')

@main_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Request password-reset email."""
    if request.method == 'POST':
        res = auth_service.send_password_reset(request.form['email'])
        return render_template('forgot_password.html', message=res.get('message'))
    return render_template('forgot_password.html')

@main_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """Reset password using token."""
    if request.method == 'POST':
        res = auth_service.reset_password(token, request.form['password'])
        if res.get('success'):
            return redirect(url_for('main.login'))
        return render_template('reset_password.html', token=token, error=res.get('error'))
    return render_template('reset_password.html', token=token)

@main_bp.route('/logout')
def logout():
    """Log out current user."""
    try:
        auth_service.logout_user()
    except Exception as e:
        logger.error(f"Logout error: {e}")
    return redirect(url_for('main.index'))

# --- PROFILE ROUTE ---

@main_bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    """View or update profile."""
    user = get_current_user()
    if request.method == 'POST':
        res = auth_service.update_profile(user['user_id'], request.form)
        return render_template('profile.html',
                               user=user,
                               message=res.get('message'),
                               error=res.get('error'))
    return render_template('profile.html', user=user)

# --- STATIC CONTENT PAGES ---

@main_bp.route('/<string:page>')
def static_page(page):
    """Render standalone <page>.html if present, else 404."""
    tpl_dir = current_app.jinja_loader.searchpath[0]
    if f"{page}.html" in os.listdir(tpl_dir):
        return render_template(f"{page}.html")
    return render_template('404.html'), 404
