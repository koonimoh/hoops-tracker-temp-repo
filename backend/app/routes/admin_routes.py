# backend/app/routes/admin_routes.py
"""
Admin routes for system management.
"""

from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from app.auth.decorators import admin_required, permission_required
from app.auth.auth_service import auth_service, get_current_user
from app.db.supabase import supabase
from app.core.logging import logger
from app.core.cache import cache
from datetime import datetime, timedelta

admin_bp = Blueprint('admin', __name__)

@admin_bp.route('/')
@admin_required
def dashboard():
    """Admin dashboard."""
    try:
        # Get basic system statistics
        users_result = supabase.table('user_profiles').select('id', count='exact').execute()
        users_count = users_result.count or 0
        
        players_result = supabase.table('players').select('id', count='exact').execute()
        players_count = players_result.count or 0
        
        bets_result = supabase.table('bets').select('id', count='exact').execute()
        bets_count = bets_result.count or 0
        
        # Get cache stats
        cache_stats = cache.get_stats()
        
        # Simple db_stats replacement
        db_stats = {
            'users_count': users_count,
            'players_count': players_count,
            'bets_count': bets_count,
            'status': 'connected'
        }
        
        # Get recent user registrations
        recent_users_result = supabase.table('user_profiles').select(
            'id, display_name, created_at'
        ).order('created_at', desc=True).limit(10).execute()
        
        recent_users = recent_users_result.data if recent_users_result.data else []
        
        # Get active bets summary
        active_bets_result = supabase.table('bets').select(
            'status', count='exact'
        ).eq('status', 'pending').execute()
        
        active_bets_count = active_bets_result.count or 0
        
        # Get recent audit logs if table exists
        try:
            audit_logs_result = supabase.table('user_audit_log').select(
                '*'
            ).order('created_at', desc=True).limit(20).execute()
            audit_logs = audit_logs_result.data if audit_logs_result.data else []
        except Exception:
            # If audit log table doesn't exist, use empty list
            audit_logs = []
        
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
        
        # Build query - simplified to avoid complex joins
        query = supabase.table('user_profiles').select('*')
        
        if search:
            query = query.ilike('display_name', f'%{search}%')
        
        # Get users with pagination
        offset = (page - 1) * per_page
        result = query.range(offset, offset + per_page - 1).execute()
        
        users_data = result.data if result.data else []
        
        # Get available roles - simplified
        try:
            roles_result = supabase.table('user_roles').select(
                'id, name, description'
            ).eq('is_active', True).execute()
            available_roles = roles_result.data if roles_result.data else []
        except Exception:
            # If roles table doesn't exist, use empty list
            available_roles = []
        
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
        
@admin_bp.route('/invite-user', methods=['POST'])
@admin_required
def invite_user():
    """Admin invite user via email."""
    try:
        email = request.form.get('email', '').strip().lower()
        
        if not email:
            flash('Email is required.', 'error')
            return redirect(url_for('admin.users'))
        
        result = auth_service.invite_user(email)
        
        if result['success']:
            flash(result['message'], 'success')
        else:
            flash(result['error'], 'error')
        
        return redirect(url_for('admin.users'))
        
    except Exception as e:
        logger.error(f"Admin invite error: {e}")
        flash('Failed to send invitation.', 'error')
        return redirect(url_for('admin.users'))

@admin_bp.route('/data-management')
@admin_required
def data_management():
    """Data management and ETL page."""
    try:
        # Simplified ETL status - no complex ETL service
        etl_status = {
            'last_run': datetime.utcnow() - timedelta(hours=1),
            'status': 'idle',
            'next_run': datetime.utcnow() + timedelta(hours=23)
        }
        
        # Get recent ETL jobs (simplified - using placeholder data)
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
            # Simplified - just return success message
            result = {'success': True, 'message': 'Full refresh triggered (simulated)'}
        elif job_type == 'stats_update':
            # Simplified - just return success message
            result = {'success': True, 'message': 'Stats update triggered (simulated)'}
        elif job_type == 'clear_cache':
            # Clear application cache
            cache.clear()
            result = {'success': True, 'message': 'Cache cleared successfully'}
        else:
            return jsonify({'error': 'Invalid job type'}), 400
        
        # Log the admin action if audit table exists
        try:
            supabase.rpc('log_user_action', {
                'user_uuid': current_user['user_id'],
                'action_name': f'trigger_etl_{job_type}',
                'resource_name': 'system',
                'new_data': {'job_type': job_type, 'result': str(result)}
            }).execute()
        except Exception:
            # If audit function doesn't exist, just continue
            pass
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"ETL job trigger error: {e}")
        return jsonify({'error': 'Failed to trigger job'}), 500

@admin_bp.route('/system-settings')
@admin_required
def system_settings():
    """System settings and configuration page."""
    try:
        # Get current system settings (simplified hardcoded values)
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
                'default_timeout': 300,
                'type': 'in_memory',
                'current_keys': cache.get_stats().get('total_keys', 0)
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
        
        # Try to get audit logs if table exists
        try:
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
                
        except Exception:
            # If audit log table doesn't exist, use empty data
            audit_logs = []
            unique_actions = []
        
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
                             
                             
@admin_bp.route('/test-invite', methods=['GET', 'POST'])
@admin_required
def test_invite():
    """Test invite functionality."""
    if request.method == 'POST':
        email = request.form.get('email')
        result = auth_service.invite_user(email)
        return jsonify(result)
    
    return '''
    <form method="POST">
        <input type="email" name="email" placeholder="Email to invite" required>
        <button type="submit">Send Invite</button>
    </form>
    '''