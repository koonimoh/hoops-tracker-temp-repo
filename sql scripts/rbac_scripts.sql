-- Role-Based Access Control Schema for Hoops Tracker
-- This extends your existing Supabase auth.users table

-- Create roles table
CREATE TABLE user_roles (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    permissions JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create user profiles table (extends auth.users)
CREATE TABLE user_profiles (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES user_roles(id),
    
    -- Profile information
    display_name VARCHAR(100),
    avatar_url TEXT,
    bio TEXT,
    preferences JSONB DEFAULT '{}',
    
    -- Subscription and limits
    subscription_tier VARCHAR(20) DEFAULT 'free' CHECK (subscription_tier IN ('free', 'premium', 'pro')),
    bet_limit DECIMAL(10,2) DEFAULT 100.00,
    watchlist_limit INTEGER DEFAULT 10,
    
    -- Activity tracking
    last_login_at TIMESTAMPTZ,
    login_count INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    is_verified BOOLEAN DEFAULT false,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT user_profiles_unique_user UNIQUE(user_id)
);

-- Create permissions table for granular control
CREATE TABLE permissions (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    resource VARCHAR(50) NOT NULL, -- e.g., 'bets', 'players', 'admin'
    action VARCHAR(50) NOT NULL,   -- e.g., 'create', 'read', 'update', 'delete'
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create role_permissions junction table
CREATE TABLE role_permissions (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    role_id UUID NOT NULL REFERENCES user_roles(id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    granted_at TIMESTAMPZ DEFAULT NOW(),
    granted_by UUID REFERENCES auth.users(id),
    
    CONSTRAINT role_permissions_unique UNIQUE(role_id, permission_id)
);

-- Create user audit log
CREATE TABLE user_audit_log (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id),
    action VARCHAR(50) NOT NULL,
    resource VARCHAR(50),
    resource_id UUID,
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_user_profiles_user_id ON user_profiles(user_id);
CREATE INDEX idx_user_profiles_role_id ON user_profiles(role_id);
CREATE INDEX idx_user_profiles_active ON user_profiles(is_active) WHERE is_active = true;
CREATE INDEX idx_permissions_resource_action ON permissions(resource, action);
CREATE INDEX idx_role_permissions_role_id ON role_permissions(role_id);
CREATE INDEX idx_user_audit_log_user_id ON user_audit_log(user_id, created_at DESC);

-- Insert default roles
INSERT INTO user_roles (name, description, permissions) VALUES 
('admin', 'System Administrator', '{"all": true}'),
('moderator', 'Content Moderator', '{"users": ["read", "update"], "content": ["read", "update", "delete"]}'),
('premium', 'Premium User', '{"bets": ["create", "read", "update"], "watchlist": {"limit": 50}, "analytics": ["read"]}'),
('user', 'Standard User', '{"bets": ["create", "read"], "watchlist": {"limit": 10}, "players": ["read"]}'),
('guest', 'Guest User', '{"players": ["read"], "teams": ["read"]}');

-- Insert default permissions
INSERT INTO permissions (name, resource, action, description) VALUES 
-- User management
('users.read', 'users', 'read', 'View user profiles'),
('users.update', 'users', 'update', 'Update user profiles'),
('users.delete', 'users', 'delete', 'Delete user accounts'),
('users.admin', 'users', 'admin', 'Full user administration'),

-- Betting permissions
('bets.create', 'bets', 'create', 'Place new bets'),
('bets.read', 'bets', 'read', 'View betting history'),
('bets.update', 'bets', 'update', 'Modify existing bets'),
('bets.delete', 'bets', 'delete', 'Cancel/delete bets'),
('bets.admin', 'bets', 'admin', 'Manage all user bets'),

-- Player and stats permissions
('players.read', 'players', 'read', 'View player information'),
('players.update', 'players', 'update', 'Update player data'),
('stats.read', 'stats', 'read', 'View statistics'),
('stats.update', 'stats', 'update', 'Update statistics'),

-- Watchlist permissions
('watchlist.create', 'watchlist', 'create', 'Add to watchlist'),
('watchlist.read', 'watchlist', 'read', 'View watchlist'),
('watchlist.update', 'watchlist', 'update', 'Modify watchlist'),
('watchlist.delete', 'watchlist', 'delete', 'Remove from watchlist'),

-- System permissions
('system.admin', 'system', 'admin', 'Full system administration'),
('analytics.read', 'analytics', 'read', 'View analytics dashboards'),
('reports.generate', 'reports', 'generate', 'Generate reports');

-- Function to check user permissions
CREATE OR REPLACE FUNCTION user_has_permission(
    user_uuid UUID,
    permission_name VARCHAR(100)
)
RETURNS BOOLEAN AS $$
DECLARE
    has_perm BOOLEAN := FALSE;
    user_role_perms JSONB;
BEGIN
    -- Check if user has admin role (has all permissions)
    SELECT ur.permissions INTO user_role_perms
    FROM user_profiles up
    JOIN user_roles ur ON up.role_id = ur.id
    WHERE up.user_id = user_uuid AND up.is_active = true AND ur.is_active = true;
    
    -- If user has admin permissions
    IF user_role_perms ? 'all' AND user_role_perms->>'all' = 'true' THEN
        RETURN TRUE;
    END IF;
    
    -- Check specific permission through role_permissions
    SELECT EXISTS(
        SELECT 1 
        FROM user_profiles up
        JOIN role_permissions rp ON up.role_id = rp.role_id
        JOIN permissions p ON rp.permission_id = p.id
        WHERE up.user_id = user_uuid 
        AND p.name = permission_name
        AND up.is_active = true
    ) INTO has_perm;
    
    RETURN has_perm;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to get user's role and permissions
CREATE OR REPLACE FUNCTION get_user_permissions(user_uuid UUID)
RETURNS TABLE (
    role_name VARCHAR(50),
    permission_name VARCHAR(100),
    resource VARCHAR(50),
    action VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ur.name as role_name,
        p.name as permission_name,
        p.resource,
        p.action
    FROM user_profiles up
    JOIN user_roles ur ON up.role_id = ur.id
    JOIN role_permissions rp ON ur.id = rp.role_id
    JOIN permissions p ON rp.permission_id = p.id
    WHERE up.user_id = user_uuid 
    AND up.is_active = true 
    AND ur.is_active = true;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Row Level Security (RLS) policies

-- Enable RLS on sensitive tables
ALTER TABLE bets ENABLE ROW LEVEL SECURITY;
ALTER TABLE watchlists ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_profiles ENABLE ROW LEVEL SECURITY;

-- Bets RLS: Users can only see their own bets, admins can see all
CREATE POLICY "Users can view their own bets" ON bets
    FOR SELECT USING (
        user_id = auth.uid() OR 
        user_has_permission(auth.uid(), 'bets.admin')
    );

CREATE POLICY "Users can create their own bets" ON bets
    FOR INSERT WITH CHECK (
        user_id = auth.uid() AND 
        user_has_permission(auth.uid(), 'bets.create')
    );

CREATE POLICY "Users can update their own bets" ON bets
    FOR UPDATE USING (
        user_id = auth.uid() OR 
        user_has_permission(auth.uid(), 'bets.admin')
    );

-- Watchlists RLS: Users can only manage their own watchlists
CREATE POLICY "Users can view their own watchlists" ON watchlists
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY "Users can create their own watchlists" ON watchlists
    FOR INSERT WITH CHECK (
        user_id = auth.uid() AND 
        user_has_permission(auth.uid(), 'watchlist.create')
    );

-- User profiles RLS: Users can view their own profile, admins can view all
CREATE POLICY "Users can view their own profile" ON user_profiles
    FOR SELECT USING (
        user_id = auth.uid() OR 
        user_has_permission(auth.uid(), 'users.admin')
    );

CREATE POLICY "Users can update their own profile" ON user_profiles
    FOR UPDATE USING (user_id = auth.uid());

-- Function to create user profile on signup (trigger)
CREATE OR REPLACE FUNCTION create_user_profile()
RETURNS TRIGGER AS $$
DECLARE
    default_role_id UUID;
BEGIN
    -- Get default user role ID
    SELECT id INTO default_role_id 
    FROM user_roles 
    WHERE name = 'user' AND is_active = true;
    
    -- Create user profile
    INSERT INTO user_profiles (user_id, role_id, display_name)
    VALUES (NEW.id, default_role_id, COALESCE(NEW.raw_user_meta_data->>'full_name', split_part(NEW.email, '@', 1)));
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Trigger to create user profile on auth.users insert
CREATE TRIGGER create_user_profile_trigger
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION create_user_profile();

-- Function to log user actions (for audit trail)
CREATE OR REPLACE FUNCTION log_user_action(
    user_uuid UUID,
    action_name VARCHAR(50),
    resource_name VARCHAR(50) DEFAULT NULL,
    resource_uuid UUID DEFAULT NULL,
    old_data JSONB DEFAULT NULL,
    new_data JSONB DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO user_audit_log (
        user_id, action, resource, resource_id, 
        old_values, new_values, ip_address
    ) VALUES (
        user_uuid, action_name, resource_name, resource_uuid,
        old_data, new_data, inet_client_addr()
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;