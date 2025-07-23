"""
Setup script for Hoops Tracker application.
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
    """Set up the development environment."""
    print("🏀 Setting up Hoops Tracker Development Environment")
    print("=" * 50)
    
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
    
    # Install Python dependencies
    if not run_command("pip install -r requirements.txt", "Installing Python dependencies"):
        return False
    
    # Install Node.js dependencies (for frontend build)
    if Path('package.json').exists():
        if not run_command("npm install", "Installing Node.js dependencies"):
            return False
        
        # Build CSS
        if not run_command("npm run build-css", "Building CSS"):
            return False
    
    # Create logs directory
    logs_dir = Path('logs')
    if not logs_dir.exists():
        logs_dir.mkdir()
        print("✅ Logs directory created")
    
    # Set up database (if needed)
    print("\n📊 Database Setup")
    print("Please ensure you have:")
    print("1. A Supabase project set up with the provided SQL schema")
    print("2. Redis running locally or accessible remotely")
    print("3. Updated your .env file with the correct database URLs")
    
    print("\n🎉 Setup completed!")
    print("\nNext steps:")
    print("1. Edit .env file with your Supabase credentials")
    print("2. Run the SQL scripts in sql_scripts/ folder in your Supabase SQL editor")
    print("3. Start Redis: redis-server")
    print("4. Start Celery: celery -A app.tasks.celery_app worker --loglevel=info")
    print("5. Start the app: python run_dev.py")
    
    return True

if __name__ == "__main__":
    success = setup_environment()
    sys.exit(0 if success else 1)
