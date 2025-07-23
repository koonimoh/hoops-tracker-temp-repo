"""
Simplified setup script for Hoops Tracker application.
"""

import os
import sys
import subprocess
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False

def setup_environment():
    """Set up the simplified development environment."""
    print("🏀 Setting up Hoops Tracker Development Environment (Simplified)")
    print("=" * 60)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    
    # Create .env file if it doesn't exist
    env_example = Path('.env.example')
    env_file = Path('.env')
    
    if env_example.exists() and not env_file.exists():
        print("📄 Creating .env file from template...")
        env_file.write_text(env_example.read_text())
        print("✅ .env file created. Please edit it with your configuration.")
    elif not env_file.exists():
        print("⚠️  No .env file found. Please create one with your Supabase credentials.")
    
    # Install Python dependencies
    if not run_command("pip install -r requirements.txt", "Installing Python dependencies"):
        return False
    
    # Create logs directory
    logs_dir = Path('logs')
    if not logs_dir.exists():
        logs_dir.mkdir()
        print("✅ Logs directory created")
    
    # Create flask_session directory for session storage
    session_dir = Path('flask_session')
    if not session_dir.exists():
        session_dir.mkdir()
        print("✅ Session directory created")
    
    print("\n📊 Database Setup")
    print("Please ensure you have:")
    print("1. A Supabase project set up")
    print("2. Updated your .env file with the correct Supabase credentials")
    print("3. Run any necessary SQL scripts in your Supabase SQL editor")
    
    print("\n🎉 Simplified setup completed!")
    print("\nNext steps:")
    print("1. Edit .env file with your Supabase credentials")
    print("2. Start the app: python run_dev.py")
    print("\nNote: This simplified version uses:")
    print("- In-memory caching (no Redis required)")
    print("- Filesystem sessions (no Redis required)")
    print("- Basic logging to files")
    
    return True

if __name__ == "__main__":
    success = setup_environment()
    sys.exit(0 if success else 1)