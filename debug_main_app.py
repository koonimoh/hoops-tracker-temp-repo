# debug_main_app.py
"""
Debug script to test main app components in isolation
"""
import os
import sys
import logging
from dotenv import load_dotenv

def test_environment():
    """Test if environment variables are loaded"""
    load_dotenv()
    
    required_vars = ['SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        return False
    
    print("✅ Environment variables loaded successfully")
    return True

def test_supabase_connection():
    """Test Supabase connection"""
    try:
        from supabase_client import SupabaseClient
        supabase = SupabaseClient()
        
        # Test a simple query
        teams = supabase.get_all_teams()
        print(f"✅ Supabase connection successful. Found {len(teams)} teams")
        return True, supabase
    except Exception as e:
        print(f"❌ Supabase connection failed: {str(e)}")
        return False, None

def test_nba_service():
    """Test NBA service initialization"""
    try:
        from nba_service import NBAService
        nba = NBAService()
        print("✅ NBA service initialized successfully")
        return True, nba
    except Exception as e:
        print(f"❌ NBA service initialization failed: {str(e)}")
        return False, None

def test_service_integration():
    """Test service integration"""
    env_ok = test_environment()
    if not env_ok:
        return False
    
    supabase_ok, supabase = test_supabase_connection()
    if not supabase_ok:
        return False
    
    nba_ok, nba = test_nba_service()
    if not nba_ok:
        return False
    
    try:
        nba.set_supabase_client(supabase)
        print("✅ Service integration successful")
        
        # Test a simple sync operation
        print("🔄 Testing teams sync...")
        result = nba.sync_teams()
        print(f"✅ Teams sync result: {result}")
        
        return True
    except Exception as e:
        print(f"❌ Service integration failed: {str(e)}")
        return False

def test_flask_app_creation():
    """Test Flask app creation without running it"""
    try:
        # Import after environment is loaded
        from app import create_app
        
        app = create_app()
        print("✅ Flask app created successfully")
        
        # Test app context
        with app.app_context():
            print("✅ App context works")
            
            # Test service access
            if hasattr(app, 'supabase') and hasattr(app, 'nba_service'):
                print("✅ Services attached to app")
            else:
                print("❌ Services not properly attached to app")
                return False
        
        return True
    except Exception as e:
        print(f"❌ Flask app creation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    logging.basicConfig(level=logging.INFO)
    
    print("🔍 Debugging main application components...\n")
    
    print("1. Testing service integration (like test_sync.py):")
    integration_ok = test_service_integration()
    
    print("\n2. Testing Flask app creation:")
    flask_ok = test_flask_app_creation()
    
    print(f"\n📊 Results:")
    print(f"   Service Integration: {'✅ PASS' if integration_ok else '❌ FAIL'}")
    print(f"   Flask App Creation: {'✅ PASS' if flask_ok else '❌ FAIL'}")
    
    if integration_ok and flask_ok:
        print("\n✅ All tests passed! The issue might be in:")
        print("   - Authentication/session management")
        print("   - Request handling")
        print("   - Cache operations")
        print("   - Parallel processing")
    else:
        print("\n❌ Found issues in basic components")

if __name__ == "__main__":
    main()