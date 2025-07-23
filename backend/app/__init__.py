from flask import Flask, render_template, request, redirect, session, url_for, jsonify, flash
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv('.env')

def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')
    
    @app.route('/')
    def index():
        return render_template('dashboard.html')
    
    @app.route('/players')
    def players():
        return render_template('players.html', players=[])
    
    @app.route('/bets')
    def bets():
        return render_template('bets.html', bets=[], stats={})
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8000, debug=True)