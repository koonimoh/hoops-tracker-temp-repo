// NBA Betting Dashboard - Main Frontend JavaScript

class NBAPlatform {
    constructor() {
        this.currentView = 'dashboard';
        this.socket = null;
        this.data = {
            games: [],
            players: [],
            watchlist: [],
            bets: []
        };
        this.init();
    }

    async init() {
        await this.loadData();
        this.setupEventListeners();
        this.initializeWebSocket();
        this.renderCurrentView();
    }

    async loadData() {
        try {
            const [games, players, watchlist, bets] = await Promise.all([
                fetch('/api/games').then(r => r.json()),
                fetch('/api/players').then(r => r.json()),
                fetch('/api/watchlist').then(r => r.json()),
                fetch('/api/bets').then(r => r.json())
            ]);
            
            this.data = { games, players, watchlist, bets };
        } catch (error) {
            console.error('Failed to load data:', error);
        }
    }

    setupEventListeners() {
        // Navigation
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                const view = e.target.dataset.view;
                this.switchView(view);
            });
        });

        // Refresh button
        document.getElementById('refresh-btn')?.addEventListener('click', () => {
            this.loadData().then(() => this.renderCurrentView());
        });
    }

    initializeWebSocket() {
        this.socket = new WebSocket(`ws://${window.location.host}/ws`);
        
        this.socket.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleRealTimeUpdate(data);
        };

        this.socket.onclose = () => {
            setTimeout(() => this.initializeWebSocket(), 5000);
        };
    }

    handleRealTimeUpdate(data) {
        if (data.type === 'game_update') {
            this.updateGameData(data.game);
        } else if (data.type === 'odds_update') {
            this.updateOddsData(data.odds);
        }
        this.renderCurrentView();
    }

    switchView(view) {
        this.currentView = view;
        document.querySelectorAll('.nav-item').forEach(item => {
            item.classList.remove('active');
        });
        document.querySelector(`[data-view="${view}"]`).classList.add('active');
        this.renderCurrentView();
    }

    renderCurrentView() {
        const content = document.getElementById('main-content');
        
        switch(this.currentView) {
            case 'dashboard':
                content.innerHTML = this.renderDashboard();
                break;
            case 'players':
                content.innerHTML = this.renderPlayers();
                break;
            case 'watchlist':
                content.innerHTML = this.renderWatchlist();
                break;
        }
    }

    renderDashboard() {
        const todayGames = this.data.games.filter(game => 
            new Date(game.date).toDateString() === new Date().toDateString()
        );

        return `
            <div class="dashboard-grid">
                <div class="stat-card">
                    <h3>Today's Games</h3>
                    <div class="stat-value">${todayGames.length}</div>
                </div>
                <div class="stat-card">
                    <h3>Active Bets</h3>
                    <div class="stat-value">${this.data.bets.filter(b => b.status === 'active').length}</div>
                </div>
                <div class="stat-card">
                    <h3>Watchlist</h3>
                    <div class="stat-value">${this.data.watchlist.length}</div>
                </div>
            </div>
            <div class="games-section">
                <h2>Today's Games</h2>
                <div class="games-grid">
                    ${todayGames.map(game => this.renderGameCard(game)).join('')}
                </div>
            </div>
        `;
    }

    renderGameCard(game) {
        return `
            <div class="game-card">
                <div class="teams">
                    <div class="team">${game.home_team}</div>
                    <div class="vs">vs</div>
                    <div class="team">${game.away_team}</div>
                </div>
                <div class="game-time">${new Date(game.date).toLocaleTimeString()}</div>
                <div class="odds">
                    <span>O/U: ${game.total_points || 'N/A'}</span>
                    <span>Spread: ${game.spread || 'N/A'}</span>
                </div>
            </div>
        `;
    }

    renderPlayers() {
        return `
            <div class="players-header">
                <h2>Player Performance</h2>
                <input type="text" id="player-search" placeholder="Search players..." />
            </div>
            <div class="players-grid">
                ${this.data.players.map(player => this.renderPlayerCard(player)).join('')}
            </div>
        `;
    }

    renderPlayerCard(player) {
        return `
            <div class="player-card">
                <div class="player-name">${player.name}</div>
                <div class="player-team">${player.team}</div>
                <div class="player-stats">
                    <div class="stat">
                        <span class="label">PPG:</span>
                        <span class="value">${player.points_per_game}</span>
                    </div>
                    <div class="stat">
                        <span class="label">RPG:</span>
                        <span class="value">${player.rebounds_per_game}</span>
                    </div>
                    <div class="stat">
                        <span class="label">APG:</span>
                        <span class="value">${player.assists_per_game}</span>
                    </div>
                </div>
                <button class="btn-watch" onclick="platform.addToWatchlist('${player.id}')">
                    Add to Watch
                </button>
            </div>
        `;
    }

    renderWatchlist() {
        return `
            <div class="watchlist-header">
                <h2>Your Watchlist</h2>
            </div>
            <div class="watchlist-items">
                ${this.data.watchlist.map(item => this.renderWatchlistItem(item)).join('')}
            </div>
        `;
    }

    renderWatchlistItem(item) {
        return `
            <div class="watchlist-item">
                <div class="item-info">
                    <div class="item-name">${item.name}</div>
                    <div class="item-type">${item.type}</div>
                </div>
                <div class="item-actions">
                    <button class="btn-remove" onclick="platform.removeFromWatchlist('${item.id}')">
                        Remove
                    </button>
                </div>
            </div>
        `;
    }

    async addToWatchlist(itemId) {
        try {
            await fetch('/api/watchlist', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ item_id: itemId })
            });
            await this.loadData();
            this.renderCurrentView();
        } catch (error) {
            console.error('Failed to add to watchlist:', error);
        }
    }

    async removeFromWatchlist(itemId) {
        try {
            await fetch(`/api/watchlist/${itemId}`, { method: 'DELETE' });
            await this.loadData();
            this.renderCurrentView();
        } catch (error) {
            console.error('Failed to remove from watchlist:', error);
        }
    }
}

// Initialize the platform when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.platform = new NBAPlatform();
});