// static/js/nba-utils.js - Utility functions for NBA app

/**
 * NBA App Utilities
 * Enhanced utilities for the NBA tracking application
 */

const NBAUtils = {
    /**
     * Image handling utilities
     */
    images: {
        /**
         * Get player headshot URL with fallbacks
         */
        getPlayerHeadshotUrl(nbaPlayerId, size = '260x190') {
            if (!nbaPlayerId) return '/static/img/default-player.png';
            
            const sizes = {
                'small': '260x190',
                'medium': '1040x760',
                'large': '1040x760'
            };
            
            const actualSize = sizes[size] || size;
            return `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/${actualSize}/${nbaPlayerId}.png`;
        },

        /**
         * Get team logo URL with fallbacks
         */
        getTeamLogoUrl(nbaTeamId, type = 'primary') {
            if (!nbaTeamId) return '/static/img/default-team.png';
            
            const types = {
                'primary': 'primary/L/logo.svg',
                'global': 'global/L/logo.svg',
                'secondary': 'secondary/L/logo.svg'
            };
            
            const logoType = types[type] || types.primary;
            return `https://cdn.nba.com/logos/nba/${nbaTeamId}/${logoType}`;
        },

        /**
         * Handle image loading errors with automatic fallbacks
         */
        handleImageError(img, type = 'player') {
            if (img.hasAttribute('data-error-handled')) return;
            
            img.setAttribute('data-error-handled', 'true');
            
            if (type === 'player') {
                this.handlePlayerImageError(img);
            } else if (type === 'team') {
                this.handleTeamImageError(img);
            }
        },

        /**
         * Handle player image errors with multiple fallback attempts
         */
        handlePlayerImageError(img) {
            const playerId = this.extractPlayerIdFromUrl(img.src);
            const currentAttempt = parseInt(img.getAttribute('data-attempt') || '0');
            
            const fallbackUrls = [
                `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`,
                `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${playerId}.png`,
                `https://stats.nba.com/media/img/players/headshots/${playerId}.jpg`,
                '/static/img/default-player.png'
            ];
            
            if (currentAttempt < fallbackUrls.length - 1) {
                img.setAttribute('data-attempt', currentAttempt + 1);
                img.src = fallbackUrls[currentAttempt + 1];
            } else {
                img.src = '/static/img/default-player.png';
                img.alt = 'Player headshot unavailable';
            }
        },

        /**
         * Handle team logo errors with multiple fallback attempts
         */
        handleTeamImageError(img) {
            const teamId = this.extractTeamIdFromUrl(img.src);
            const currentAttempt = parseInt(img.getAttribute('data-attempt') || '0');
            
            const fallbackUrls = [
                `https://cdn.nba.com/logos/nba/${teamId}/global/L/logo.svg`,
                `https://stats.nba.com/media/img/teams/logos/${teamId}_logo.svg`,
                `https://cdn.nba.com/logos/nba/${teamId}/primary/D/logo.svg`,
                '/static/img/default-team.png'
            ];
            
            if (currentAttempt < fallbackUrls.length - 1) {
                img.setAttribute('data-attempt', currentAttempt + 1);
                img.src = fallbackUrls[currentAttempt + 1];
            } else {
                img.src = '/static/img/default-team.png';
                img.alt = 'Team logo unavailable';
            }
        },

        /**
         * Extract player ID from image URL
         */
        extractPlayerIdFromUrl(url) {
            const match = url.match(/(?:headshots|players).*?(\d+)\.(?:png|jpg)/);
            return match ? match[1] : null;
        },

        /**
         * Extract team ID from image URL
         */
        extractTeamIdFromUrl(url) {
            const match = url.match(/(?:logos|teams).*?(\d+).*?\.(?:svg|png)/);
            return match ? match[1] : null;
        }
    },

    /**
     * Statistics calculation utilities
     */
    stats: {
        /**
         * Calculate team averages from player stats
         */
        calculateTeamAverages(players) {
            if (!players || players.length === 0) {
                return {
                    avgPoints: 0,
                    avgRebounds: 0,
                    avgAssists: 0,
                    totalPlayers: 0
                };
            }

            const totals = players.reduce((acc, player) => {
                const stats = player.players || player;
                acc.points += parseFloat(stats.avg_points || 0);
                acc.rebounds += parseFloat(stats.avg_rebounds || 0);
                acc.assists += parseFloat(stats.avg_assists || 0);
                return acc;
            }, { points: 0, rebounds: 0, assists: 0 });

            const count = players.length;
            
            return {
                avgPoints: (totals.points / count).toFixed(1),
                avgRebounds: (totals.rebounds / count).toFixed(1),
                avgAssists: (totals.assists / count).toFixed(1),
                totalPlayers: count
            };
        },

        /**
         * Calculate shooting percentage
         */
        calculateShootingPercentage(made, attempted) {
            if (!attempted || attempted === 0) return 0;
            return ((made / attempted) * 100).toFixed(1);
        },

        /**
         * Format player efficiency rating (PER)
         */
        calculatePER(stats) {
            // Simplified PER calculation
            const { points = 0, rebounds = 0, assists = 0, steals = 0, blocks = 0, 
                    turnovers = 0, minutes = 0, fieldGoalsMade = 0, fieldGoalsAttempted = 0 } = stats;
            
            if (minutes === 0) return 0;
            
            const per = (points + rebounds + assists + steals + blocks - turnovers - 
                        (fieldGoalsAttempted - fieldGoalsMade)) / minutes * 36;
            
            return Math.max(0, per).toFixed(1);
        },

        /**
         * Get color for stat value (good/bad indicator)
         */
        getStatColor(value, statType, threshold = null) {
            const thresholds = {
                points: { good: 15, excellent: 25 },
                rebounds: { good: 6, excellent: 10 },
                assists: { good: 4, excellent: 7 },
                fieldGoalPercentage: { good: 45, excellent: 50 },
                threePointPercentage: { good: 35, excellent: 40 }
            };
            
            const statThreshold = threshold || thresholds[statType];
            if (!statThreshold) return 'text-muted';
            
            if (value >= statThreshold.excellent) return 'text-success';
            if (value >= statThreshold.good) return 'text-warning';
            return 'text-muted';
        }
    },

    /**
     * Data formatting utilities
     */
    format: {
        /**
         * Format height from inches to feet-inches
         */
        formatHeight(inches) {
            if (!inches || inches === 0) return 'N/A';
            const feet = Math.floor(inches / 12);
            const remainingInches = inches % 12;
            return `${feet}'${remainingInches}"`;
        },

        /**
         * Format weight with units
         */
        formatWeight(lbs) {
            if (!lbs || lbs === 0) return 'N/A';
            return `${lbs} lbs`;
        },

        /**
         * Format game date
         */
        formatGameDate(dateString) {
            if (!dateString) return 'N/A';
            const date = new Date(dateString);
            return date.toLocaleDateString('en-US', { 
                month: 'short', 
                day: 'numeric' 
            });
        },

        /**
         * Format season string
         */
        formatSeason(season) {
            if (!season) return '2024-25';
            return season;
        },

        /**
         * Format plus/minus with proper sign
         */
        formatPlusMinus(value) {
            if (!value || value === 0) return '0';
            return value > 0 ? `+${value}` : `${value}`;
        },

        /**
         * Format percentage
         */
        formatPercentage(decimal, digits = 1) {
            if (decimal === null || decimal === undefined) return '0.0%';
            return `${(decimal * 100).toFixed(digits)}%`;
        }
    },

    /**
     * UI interaction utilities
     */
    ui: {
        /**
         * Show loading state for an element
         */
        showLoading(elementId, message = 'Loading...') {
            const element = document.getElementById(elementId);
            if (!element) return;
            
            element.innerHTML = `
                <div class="text-center py-4">
                    <div class="spinner-border spinner-border-sm" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="mt-2 text-muted mb-0">${message}</p>
                </div>
            `;
        },

        /**
         * Show error state for an element
         */
        showError(elementId, message = 'Error loading data') {
            const element = document.getElementById(elementId);
            if (!element) return;
            
            element.innerHTML = `
                <div class="text-center py-4 text-danger">
                    <i class="bi bi-exclamation-triangle display-4"></i>
                    <p class="mt-2 mb-0">${message}</p>
                </div>
            `;
        },

        /**
         * Show empty state for an element
         */
        showEmpty(elementId, message = 'No data available', icon = 'bi-inbox') {
            const element = document.getElementById(elementId);
            if (!element) return;
            
            element.innerHTML = `
                <div class="text-center py-4 text-muted">
                    <i class="bi ${icon} display-4"></i>
                    <p class="mt-2 mb-0">${message}</p>
                </div>
            `;
        },

        /**
         * Toggle view between grid and list
         */
        toggleView(gridId, listId, viewType) {
            const gridView = document.getElementById(gridId);
            const listView = document.getElementById(listId);
            const buttons = document.querySelectorAll('.btn-group .btn');
            
            if (!gridView || !listView) return;
            
            buttons.forEach(btn => btn.classList.remove('active'));
            
            if (viewType === 'grid') {
                gridView.style.display = 'block';
                listView.style.display = 'none';
                buttons[0]?.classList.add('active');
            } else {
                gridView.style.display = 'none';
                listView.style.display = 'block';
                buttons[1]?.classList.add('active');
            }
        },

        /**
         * Update stat elements with data
         */
        updateStatElements(playerId, stats) {
            const elements = document.querySelectorAll(`[data-player-id="${playerId}"]`);
            
            elements.forEach(element => {
                const statType = element.dataset.stat;
                let value = '-';
                
                switch (statType) {
                    case 'points':
                        value = (stats.avg_points || 0).toFixed(1);
                        break;
                    case 'rebounds':
                        value = (stats.avg_rebounds || 0).toFixed(1);
                        break;
                    case 'assists':
                        value = (stats.avg_assists || 0).toFixed(1);
                        break;
                    case 'fieldGoalPercentage':
                        value = this.parent.format.formatPercentage(stats.field_goal_percentage);
                        break;
                }
                
                element.textContent = value;
                element.className += ` ${this.parent.stats.getStatColor(parseFloat(value), statType)}`;
            });
        }
    },

    /**
     * API utilities
     */
    api: {
        /**
         * Base API request with error handling
         */
        async request(url, options = {}) {
            try {
                const response = await fetch(url, {
                    headers: {
                        'Content-Type': 'application/json',
                        ...options.headers
                    },
                    ...options
                });
                
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                
                return await response.json();
            } catch (error) {
                console.error('API request failed:', error);
                throw error;
            }
        },

        /**
         * Search players with debouncing
         */
        searchPlayers: this.debounce(async function(query, limit = 10) {
            if (query.length < 2) return { success: true, data: { players: [] } };
            
            return await this.request(`/api/search?q=${encodeURIComponent(query)}&type=players&limit=${limit}`);
        }, 300),

        /**
         * Get player stats with caching
         */
        async getPlayerStats(playerId) {
            const cacheKey = `player_stats_${playerId}`;
            const cached = this.getFromCache(cacheKey);
            
            if (cached) return cached;
            
            const result = await this.request(`/api/players/${playerId}`);
            
            if (result.success) {
                this.setCache(cacheKey, result, 5 * 60 * 1000); // 5 minutes
            }
            
            return result;
        },

        /**
         * Simple caching mechanism
         */
        cache: new Map(),
        
        setCache(key, value, ttl = 60000) {
            const expiry = Date.now() + ttl;
            this.cache.set(key, { value, expiry });
        },
        
        getFromCache(key) {
            const cached = this.cache.get(key);
            if (!cached) return null;
            
            if (Date.now() > cached.expiry) {
                this.cache.delete(key);
                return null;
            }
            
            return cached.value;
        }
    },

    /**
     * Utility functions
     */
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func.apply(this, args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    /**
     * Initialize all utilities
     */
    init() {
        // Set up global image error handlers
        document.addEventListener('error', (e) => {
            if (e.target.tagName === 'IMG') {
                const img = e.target;
                if (img.src.includes('nba.com') || img.src.includes('ak-static')) {
                    const type = img.alt.toLowerCase().includes('logo') ? 'team' : 'player';
                    this.images.handleImageError(img, type);
                }
            }
        }, true);

        // Set up UI references for nested objects
        this.ui.parent = this;
        
        console.log('NBA Utils initialized');
    }
};

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    NBAUtils.init();
});

// Make available globally
window.NBAUtils = NBAUtils;