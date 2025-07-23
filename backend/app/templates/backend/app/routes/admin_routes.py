# backend/app/routes/admin_routes.py
"""
Admin routes for system management.
"""

from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from app.auth.decorators import admin_required, permission_required
from app.auth.auth_service import auth_service, get_current_user
from app.services.etl_service import etl_service
from app.services.cache_service import cache_service
from app.db.supabase import supabase, get_db_stats
from app.core.logging import logger
from datetime import datetime, timedelta

admin_bp = Blueprint('admin', __name__)

@admin_bp.route('/')
@admin_required
def dashboard():
    """Admin dashboard."""
    try:
        # Get system statistics
        db_stats = get_db_stats()
        cache_stats = cache_service.get_cache_statistics()
        
        # Get recent user registrations
        recent_users_result = supabase.table('user_profiles').select(
            '*, user_roles(name)'
        ).order('created_at', desc=True).limit(10).execute()
        
        recent_users = recent_users_result.data if recent_users_result.data else []
        
        # Get active bets summary
        active_bets_result = supabase.table('bets').select(
            'status', count='exact'
        ).eq('status', 'pending').execute()
        
        active_bets_count = active_bets_result.count or 0
        
        # Get recent audit logs
        audit_logs_result = supabase.table('user_audit_log').select(
            '*'
        ).order('created_at', desc=True).limit(20).execute()
        
        audit_logs = audit_logs_result.data if audit_logs_result.data else []
        
        return render_template('admin/dashboard.html',
                             db_stats=db_stats,
                             cache_stats=cache_stats,
                             recent_users=recent_users,
                             active_bets_count=active_bets_count,
                             audit_logs=audit_logs)
                             
    except Exception as e:
        logger.error(f"Admin dashboard error: {e}")
        return render_template('admin/dashboard.html', error="Failed to load admin data")

@admin_bp.route('/users')
@admin_required
def users():
    """User management page."""
    try:
        page = int(request.args.get('page', 1))
        per_page = 20
        search = request.args.get('search', '')
        
        # Build query
        query = supabase.table('user_profiles').select(
            '''*, user_roles(name, description),
               auth.users!inner(email, created_at)'''
        )
        
        if search:
            query = query.ilike('auth.users.email', f'%{search}%')
        
        # Get users with pagination
        offset = (page - 1) * per_page
        result = query.range(offset, offset + per_page - 1).execute()
        
        users_data = result.data if result.data else []
        
        # Get available roles
        roles_result = supabase.table('user_roles').select(
            'id, name, description'
        ).eq('is_active', True).execute()
        
        available_roles = roles_result.data if roles_result.data else []
        
        return render_template('admin/users.html',
                             users=users_data,
                             available_roles=available_roles,
                             search=search,
                             page=page)
                             
    except Exception as e:
        logger.error(f"Admin users page error: {e}")
        return render_template('admin/users.html', users=[], available_roles=[])

@admin_bp.route('/users/<user_id>/role', methods=['POST'])
@admin_required
def update_user_role(user_id):
    """Update user's role."""
    try:
        current_user = get_current_user()
        new_role = request.form.get('role')
        
        if not new_role:
            flash('Role is required.', 'error')
            return redirect(url_for('admin.users'))
        
        success = auth_service.update_user_role(user_id, new_role, current_user['user_id'])
        
        if success:
            flash('User role updated successfully.', 'success')
        else:
            flash('Failed to update user role.', 'error')
        
        return redirect(url_for('admin.users'))
        
    except Exception as e:
        logger.error(f"Update user role error: {e}")
        flash('An error occurred while updating user role.', 'error')
        return redirect(url_for('admin.users'))

@admin_bp.route('/data-management')
@admin_required
def data_management():
    """Data management and ETL page."""
    try:
        # Get ETL status
        etl_status = etl_service.get_etl_status()
        
        # Get recent ETL jobs (you would track these in a separate table)
        # For now, we'll show placeholder data
        recent_jobs = [
            {
                'id': '1',
                'type': 'player_stats_update',
                'status': 'completed',
                'started_at': datetime.utcnow() - timedelta(hours=2),
                'completed_at': datetime.utcnow() - timedelta(hours=1, minutes=45),
                'records_processed': 1250
            },
            {
                'id': '2', 
                'type': 'team_standings_sync',
                'status': 'completed',
                'started_at': datetime.utcnow() - timedelta(hours=6),
                'completed_at': datetime.utcnow() - timedelta(hours=5, minutes=50),
                'records_processed': 30
            }
        ]
        
        return render_template('admin/data_management.html',
                             etl_status=etl_status,
                             recent_jobs=recent_jobs)
                             
    except Exception as e:
        logger.error(f"Data management page error: {e}")
        return render_template('admin/data_management.html', 
                             etl_status={}, recent_jobs=[])

@admin_bp.route('/api/etl/<job_type>', methods=['POST'])
@admin_required
def trigger_etl_job(job_type):
    """API endpoint to trigger ETL jobs."""
    try:
        current_user = get_current_user()
        
        if job_type == 'full_refresh':
            # Trigger full ETL pipeline
            result = etl_service.full_etl_pipeline(include_stats=True)
        elif job_type == 'stats_update':
            # Trigger incremental stats update
            result = etl_service.incremental_stats_update()
        elif job_type == 'clear_cache':
            # Clear application cache
            cache_service.cache_cleanup()
            result = {'success': True, 'message': 'Cache cleared successfully'}
        else:
            return jsonify({'error': 'Invalid job type'}), 400
        
        # Log the admin action
        supabase.rpc('log_user_action', {
            'user_uuid': current_user['user_id'],
            'action_name': f'trigger_etl_{job_type}',
            'resource_name': 'system',
            'new_data': {'job_type': job_type, 'result': str(result)}
        }).execute()
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"ETL job trigger error: {e}")
        return jsonify({'error': 'Failed to trigger job'}), 500

@admin_bp.route('/system-settings')
@admin_required
def system_settings():
    """System settings and configuration page."""
    try:
        # Get current system settings (you'd store these in a settings table)
        settings = {
            'bet_limits': {
                'free_tier': 100.00,
                'premium_tier': 1000.00,
                'pro_tier': 10000.00
            },
            'watchlist_limits': {
                'free_tier': 10,
                'premium_tier': 50,
                'pro_tier': 200
            },
            'cache_settings': {
                'default_timeout': 3600,
                'max_cache_size': '1GB'
            },
            'rate_limits': {
                'api_calls_per_minute': 60,
                'search_calls_per_minute': 30
            }
        }
        
        return render_template('admin/system_settings.html', settings=settings)
        
    except Exception as e:
        logger.error(f"System settings page error: {e}")
        return render_template('admin/system_settings.html', settings={})

@admin_bp.route('/audit-logs')
@admin_required
def audit_logs():
    """Audit logs page."""
    try:
        page = int(request.args.get('page', 1))
        per_page = 50
        action_filter = request.args.get('action', '')
        user_filter = request.args.get('user', '')
        
        # Build query
        query = supabase.table('user_audit_log').select('*')
        
        if action_filter:
            query = query.eq('action', action_filter)
        
        if user_filter:
            query = query.eq('user_id', user_filter)
        
        # Get logs with pagination
        offset = (page - 1) * per_page
        result = query.order('created_at', desc=True).range(
            offset, offset + per_page - 1
        ).execute()
        
        audit_logs = result.data if result.data else []
        
        # Get unique actions for filter dropdown
        actions_result = supabase.table('user_audit_log').select(
            'action'
        ).execute()
        
        unique_actions = []
        if actions_result.data:
            unique_actions = list(set(log['action'] for log in actions_result.data))
            unique_actions.sort()
        
        return render_template('admin/audit_logs.html',
                             audit_logs=audit_logs,
                             unique_actions=unique_actions,
                             action_filter=action_filter,
                             user_filter=user_filter,
                             page=page)
                             
    except Exception as e:
        logger.error(f"Audit logs page error: {e}")
        return render_template('admin/audit_logs.html', 
                             audit_logs=[], unique_actions=[])