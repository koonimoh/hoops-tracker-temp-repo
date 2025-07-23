// Simple NBA Platform - Main JavaScript
class SimplePlatform {
    constructor() {
        this.data = {
            games: [],
            players: [],
            watchlist: [],
            bets: []
        };
        this.init();
    }

    init() {
        // Only initialize if we're on a page that needs the platform
        if (document.getElementById('main-content')) {
            this.loadData();
            this.setupEventListeners();
        }
    }

    async loadData() {
        try {
            // Load games
            try {
                const gamesRes = await fetch('/api/games');
                if (gamesRes.ok) {
                    const gamesData = await gamesRes.json();
                    this.data.games = gamesData.games || [];
                }
            } catch (e) {
                console.log('Games API not available');
                this.data.games = [];
            }

            // Load players
            try {
                const playersRes = await fetch('/api/players');
                if (playersRes.ok) {
                    const playersData = await playersRes.json();
                    this.data.players = playersData.players || [];
                }
            } catch (e) {
                console.log('Players API not available');
                this.data.players = [];
            }

            // Load watchlist (only if user is logged in)
            try {
                const watchlistRes = await fetch('/api/watchlist');
                if (watchlistRes.ok) {
                    const watchlistData = await watchlistRes.json();
                    this.data.watchlist = watchlistData.watchlist || [];
                }
            } catch (e) {
                console.log('Watchlist API not available');
                this.data.watchlist = [];
            }

            // Load bets (only if user is logged in)
            try {
                const betsRes = await fetch('/api/bets');
                if (betsRes.ok) {
                    const betsData = await betsRes.json();
                    this.data.bets = betsData.bets || [];
                }
            } catch (e) {
                console.log('Bets API not available');
                this.data.bets = [];
            }

            console.log('Data loaded successfully');
        } catch (error) {
            console.error('Error loading data:', error);
        }
    }

    setupEventListeners() {
        // Refresh button
        const refreshBtn = document.getElementById('refresh-btn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => {
                this.loadData();
            });
        }

        // Setup form handlers
        this.setupFormHandlers();
    }

    setupFormHandlers() {
        // Watchlist forms
        const watchlistForms = document.querySelectorAll('.add-to-watchlist-form');
        watchlistForms.forEach(form => {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                const playerId = form.dataset.playerId;
                this.addToWatchlist(playerId);
            });
        });

        // Remove from watchlist buttons
        const removeButtons = document.querySelectorAll('.remove-from-watchlist');
        removeButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const itemId = button.dataset.itemId;
                this.removeFromWatchlist(itemId);
            });
        });

        // Bet simulation buttons
        const simulateButtons = document.querySelectorAll('.simulate-bet');
        simulateButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.preventDefault();
                const betId = button.dataset.betId;
                this.simulateBet(betId);
            });
        });
    }

    async addToWatchlist(playerId) {
        try {
            const response = await fetch('/api/watchlist', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ player_id: playerId })
            });

            const result = await response.json();

            if (response.ok && result.success) {
                this.showMessage('Added to watchlist!', 'success');
                // Reload page to show updated watchlist
                setTimeout(() => window.location.reload(), 1000);
            } else {
                this.showMessage(result.error || 'Failed to add to watchlist', 'error');
            }
        } catch (error) {
            console.error('Error adding to watchlist:', error);
            this.showMessage('Failed to add to watchlist', 'error');
        }
    }

    async removeFromWatchlist(itemId) {
        try {
            const response = await fetch(`/api/watchlist/${itemId}`, {
                method: 'DELETE'
            });

            const result = await response.json();

            if (response.ok && result.success) {
                this.showMessage('Removed from watchlist!', 'success');
                // Remove the item from the page
                const itemElement = document.querySelector(`[data-watchlist-id="${itemId}"]`);
                if (itemElement) {
                    itemElement.remove();
                }
            } else {
                this.showMessage(result.error || 'Failed to remove from watchlist', 'error');
            }
        } catch (error) {
            console.error('Error removing from watchlist:', error);
            this.showMessage('Failed to remove from watchlist', 'error');
        }
    }

    async simulateBet(betId) {
        try {
            const response = await fetch(`/api/betting/simulate/${betId}`, {
                method: 'POST'
            });

            const result = await response.json();

            if (response.ok && result.success) {
                this.showMessage(`Bet simulated! Result: ${result.outcome}`, 'info');
                // Reload to show updated bet status
                setTimeout(() => window.location.reload(), 2000);
            } else {
                this.showMessage(result.error || 'Failed to simulate bet', 'error');
            }
        } catch (error) {
            console.error('Error simulating bet:', error);
            this.showMessage('Failed to simulate bet', 'error');
        }
    }

    showMessage(message, type = 'info') {
        // Create or update message element
        let messageEl = document.getElementById('flash-message');
        
        if (!messageEl) {
            messageEl = document.createElement('div');
            messageEl.id = 'flash-message';
            messageEl.className = 'fixed top-4 right-4 p-4 rounded-lg shadow-lg z-50';
            document.body.appendChild(messageEl);
        }

        // Set message and styling based on type
        messageEl.textContent = message;
        messageEl.className = 'fixed top-4 right-4 p-4 rounded-lg shadow-lg z-50';
        
        if (type === 'success') {
            messageEl.className += ' bg-green-500 text-white';
        } else if (type === 'error') {
            messageEl.className += ' bg-red-500 text-white';
        } else {
            messageEl.className += ' bg-blue-500 text-white';
        }

        // Show message
        messageEl.style.display = 'block';

        // Hide after 3 seconds
        setTimeout(() => {
            messageEl.style.display = 'none';
        }, 3000);
    }
}

// Utility functions
window.addToWatchlist = function(playerId) {
    if (window.platform) {
        window.platform.addToWatchlist(playerId);
    }
};

window.removeFromWatchlist = function(itemId) {
    if (window.platform) {
        window.platform.removeFromWatchlist(itemId);
    }
};

window.simulateBet = function(betId) {
    if (window.platform) {
        window.platform.simulateBet(betId);
    }
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    console.log('Hoops Tracker JavaScript loaded');
    
    // Only initialize platform on relevant pages
    const mainContent = document.getElementById('main-content');
    if (mainContent) {
        window.platform = new SimplePlatform();
    }

    // Handle any existing flash messages
    const flashMessages = document.querySelectorAll('.flash-message');
    flashMessages.forEach(msg => {
        setTimeout(() => {
            msg.style.opacity = '0';
            setTimeout(() => msg.remove(), 300);
        }, 3000);
    });
});