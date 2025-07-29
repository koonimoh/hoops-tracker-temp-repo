// static/js/roster_detail.js
class RosterDetailManager {
    constructor() {
        this.searchTimeout = null;
        this.currentRosterPlayers = new Set(); // Track current roster players
        this.init();
    }

    init() {
        this.setupViewToggle();
        this.setupPlayerSearch();
        this.setupModalEvents();
        this.loadCurrentRosterPlayers();
    }

    loadCurrentRosterPlayers() {
        // Extract player IDs from current roster to prevent duplicates
        // Check multiple possible selectors for player data
        const playerSelectors = [
            '[data-player-id]',
            '.player-card[onclick*="viewPlayer"]',
            'tr[onclick*="viewPlayer"]'
        ];
        
        let playerIds = [];
        
        for (const selector of playerSelectors) {
            const elements = document.querySelectorAll(selector);
            elements.forEach(element => {
                let playerId = null;
                
                // Try to get from data attribute first
                if (element.hasAttribute('data-player-id')) {
                    playerId = parseInt(element.getAttribute('data-player-id'));
                }
                // Try to extract from onclick attribute
                else if (element.hasAttribute('onclick')) {
                    const onclickValue = element.getAttribute('onclick');
                    const match = onclickValue.match(/viewPlayer\((\d+)\)/);
                    if (match) {
                        playerId = parseInt(match[1]);
                    }
                }
                
                if (!isNaN(playerId) && playerId > 0) {
                    playerIds.push(playerId);
                    this.currentRosterPlayers.add(playerId);
                }
            });
        }
        
        console.log('Loaded current roster players:', Array.from(this.currentRosterPlayers));
    }

    setupViewToggle() {
        const gridView = document.getElementById('gridView');
        const listView = document.getElementById('listView');
        const gridContent = document.getElementById('gridViewContent');
        const listContent = document.getElementById('listViewContent');

        if (!gridView || !listView) return;

        gridView.addEventListener('change', () => {
            if (gridView.checked) {
                gridContent.style.display = 'block';
                listContent.style.display = 'none';
            }
        });

        listView.addEventListener('change', () => {
            if (listView.checked) {
                gridContent.style.display = 'none';
                listContent.style.display = 'block';
            }
        });
    }

    setupPlayerSearch() {
        const searchInput = document.getElementById('playerSearch');
        if (!searchInput) return;

        searchInput.addEventListener('input', (e) => {
            clearTimeout(this.searchTimeout);
            const query = e.target.value.trim();

            if (query.length < 2) {
                this.showDefaultSearchState();
                return;
            }

            this.searchTimeout = setTimeout(() => {
                this.searchPlayers(query);
            }, 300);
        });
    }

    setupModalEvents() {
        const modal = document.getElementById('addPlayerModal');
        const searchInput = document.getElementById('playerSearch');

        if (!modal || !searchInput) return;

        // Focus on search input when modal opens
        modal.addEventListener('shown.bs.modal', () => {
            searchInput.focus();
            // Reload current roster players when modal opens (in case of changes)
            this.loadCurrentRosterPlayers();
        });

        // Reset search when modal closes
        modal.addEventListener('hidden.bs.modal', () => {
            searchInput.value = '';
            this.showDefaultSearchState();
        });
    }

    showDefaultSearchState() {
        const searchResults = document.getElementById('playerSearchResults');
        const searchLoading = document.getElementById('searchLoading');

        if (searchLoading) searchLoading.style.display = 'none';
        
        if (searchResults) {
            searchResults.innerHTML = `
                <div class="text-center text-muted py-4">
                    <i class="bi bi-search display-4"></i>
                    <p class="mt-2">Search for players to add</p>
                    <small class="text-muted">Type at least 2 characters to search</small>
                </div>
            `;
        }
    }

    showLoading() {
        const searchResults = document.getElementById('playerSearchResults');

        if (searchResults) {
            searchResults.innerHTML = `
                <div class="text-center py-4">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="mt-2 text-muted">Searching players...</p>
                </div>
            `;
        }
    }

    async searchPlayers(query) {
        this.showLoading();

        try {
            const response = await fetch(`/api/players?search=${encodeURIComponent(query)}&per_page=10`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            console.log('Search API response:', result);

            // Check the API response structure
            if (result.success && result.data && result.data.players) {
                if (result.data.players.length > 0) {
                    this.displaySearchResults(result.data.players);
                } else {
                    this.showNoResults(query);
                }
            } else {
                console.error('Unexpected API response structure:', result);
                this.showSearchError('No data received from server');
            }
        } catch (error) {
            console.error('Search failed:', error);
            this.showSearchError(error.message);
        }
    }

    displaySearchResults(players) {
        const searchResults = document.getElementById('playerSearchResults');
        const rosterId = this.getRosterId();

        if (!searchResults) {
            console.error('playerSearchResults element not found');
            return;
        }

        if (!rosterId) {
            console.error('Could not determine roster ID');
            this.showSearchError('Unable to determine roster ID');
            return;
        }

        const playersHtml = players.map(player => {
            const isInRoster = this.isPlayerInRoster(player.id);
            const playerName = `${player.first_name || ''} ${player.last_name || ''}`.trim();
            
            // Handle team info - check both nested and flat structures
            let teamName = 'Free Agent';
            if (player.teams && player.teams.name) {
                teamName = player.teams.name;
            } else if (player.team_name) {
                teamName = player.team_name;
            }
            
            const position = player.position || '';
            const jerseyNumber = player.jersey_number ? `#${player.jersey_number}` : '';
            
            // Handle stats safely
            const stats = [];
            if (player.avg_points && player.avg_points > 0) {
                stats.push(`${parseFloat(player.avg_points).toFixed(1)} PPG`);
            }
            if (player.avg_rebounds && player.avg_rebounds > 0) {
                stats.push(`${parseFloat(player.avg_rebounds).toFixed(1)} RPG`);
            }
            if (player.avg_assists && player.avg_assists > 0) {
                stats.push(`${parseFloat(player.avg_assists).toFixed(1)} APG`);
            }

            const buttonId = `add-btn-${player.id}`;

            return `
                <div class="card mb-2 player-search-result">
                    <div class="card-body d-flex align-items-center justify-content-between">
                        <div class="d-flex align-items-center">
                            <img src="https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/${player.nba_player_id || 0}.png" 
                                 alt="${playerName}" 
                                 width="50" height="50" 
                                 class="rounded-circle me-3"
                                 onerror="this.src='/static/img/default-player.png'">
                            <div>
                                <h6 class="mb-1">${playerName}</h6>
                                <small class="text-muted">
                                    ${teamName}
                                    ${position ? ' • ' + position : ''}
                                    ${jerseyNumber ? ' • ' + jerseyNumber : ''}
                                </small>
                                ${stats.length > 0 ? `
                                    <div class="small text-muted mt-1">
                                        ${stats.join(' • ')}
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                        <button id="${buttonId}" 
                                class="btn ${isInRoster ? 'btn-secondary' : 'btn-primary'} btn-sm" 
                                onclick="rosterManager.addPlayerToRoster(${rosterId}, ${player.id})"
                                ${isInRoster ? 'disabled' : ''}>
                            <i class="bi bi-${isInRoster ? 'check' : 'plus'}"></i> 
                            ${isInRoster ? 'In Roster' : 'Add'}
                        </button>
                    </div>
                </div>
            `;
        }).join('');

        searchResults.innerHTML = playersHtml;
    }

    showNoResults(query) {
        const searchResults = document.getElementById('playerSearchResults');
        if (searchResults) {
            searchResults.innerHTML = `
                <div class="text-center text-muted py-4">
                    <i class="bi bi-person-x display-4"></i>
                    <p class="mt-2">No players found for "${query}"</p>
                    <small class="text-muted">Try different search terms</small>
                </div>
            `;
        }
    }

    showSearchError(errorMessage = 'Unknown error') {
        const searchResults = document.getElementById('playerSearchResults');
        if (searchResults) {
            searchResults.innerHTML = `
                <div class="text-center text-danger py-4">
                    <i class="bi bi-exclamation-triangle display-4"></i>
                    <p class="mt-2">Error searching players</p>
                    <small class="text-muted">${errorMessage}</small>
                    <br>
                    <button class="btn btn-outline-primary btn-sm mt-2" onclick="rosterManager.showDefaultSearchState()">
                        Try Again
                    </button>
                </div>
            `;
        }
    }

    async addPlayerToRoster(rosterId, playerId) {
        console.log('Adding player to roster:', { rosterId, playerId });
        
        // Check if player is already in roster (client-side prevention)
        if (this.isPlayerInRoster(playerId)) {
            if (typeof HoopsTracker !== 'undefined') {
                HoopsTracker.showToast('Player is already in this roster', 'warning');
            } else {
                alert('Player is already in this roster');
            }
            return;
        }
        
        // Find the button by ID for more reliable targeting
        const button = document.getElementById(`add-btn-${playerId}`);
        const originalButtonContent = button ? button.innerHTML : '';
        
        if (button) {
            button.disabled = true;
            button.innerHTML = '<i class="bi bi-hourglass-split"></i> Adding...';
        }

        try {
            const response = await fetch(`/rosters/${rosterId}/players`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    player_id: playerId
                })
            });

            console.log('Response status:', response.status);
            const result = await response.json();
            console.log('Response result:', result);

            if (response.ok && result.success) {
                // Add to local set to prevent re-adding
                this.currentRosterPlayers.add(playerId);
                
                // Update button to show added state
                if (button) {
                    button.innerHTML = '<i class="bi bi-check"></i> Added';
                    button.classList.remove('btn-primary');
                    button.classList.add('btn-success');
                }
                
                // Show success message
                if (typeof HoopsTracker !== 'undefined') {
                    HoopsTracker.showToast('Player added to roster!', 'success');
                } else {
                    console.log('Player added successfully!');
                }
                
                // Close modal and reload after a delay
                setTimeout(() => {
                    const modal = bootstrap.Modal.getInstance(document.getElementById('addPlayerModal'));
                    if (modal) {
                        modal.hide();
                    }
                    // Reload to show updated roster
                    location.reload();
                }, 1500);
                
            } else {
                // Handle specific error cases
                let errorMessage = 'Failed to add player';
                
                if (result.error) {
                    if (result.error.includes('already in this roster') || 
                        result.error.includes('duplicate key value')) {
                        errorMessage = 'Player is already in this roster';
                        // Add to local set since it's already in the database
                        this.currentRosterPlayers.add(playerId);
                    } else if (result.error.includes('Roster is full')) {
                        errorMessage = 'Roster is full (maximum 15 players)';
                    } else {
                        errorMessage = result.error;
                    }
                } else if (response.status === 403) {
                    errorMessage = 'Access denied - check if you own this roster';
                } else if (response.status === 400) {
                    errorMessage = 'Invalid request - roster may be full';
                }
                
                throw new Error(errorMessage);
            }
        } catch (error) {
            console.error('Error adding player:', error);
            
            // Re-enable button with original state (unless it's a duplicate error)
            if (button && !error.message.includes('already in this roster')) {
                button.disabled = false;
                button.innerHTML = originalButtonContent;
                button.classList.remove('btn-success');
                button.classList.add('btn-primary');
            } else if (button && error.message.includes('already in this roster')) {
                // Update button to show it's already in roster
                button.innerHTML = '<i class="bi bi-check"></i> In Roster';
                button.classList.remove('btn-primary');
                button.classList.add('btn-secondary');
                button.disabled = true;
            }
            
            // Show error message
            if (typeof HoopsTracker !== 'undefined') {
                HoopsTracker.showToast(error.message, 'warning');
            } else {
                alert(error.message);
            }
        }
    }

    async removePlayer(event, rosterId, playerId) {
        event.stopPropagation();

        if (!confirm('Remove this player from the roster?')) {
            return;
        }

        try {
            const response = await fetch(`/rosters/${rosterId}/players?player_id=${playerId}`, {
                method: 'DELETE'
            });

            const result = await response.json();

            if (result.success) {
                // Remove from local set
                this.currentRosterPlayers.delete(playerId);
                
                if (typeof HoopsTracker !== 'undefined') {
                    HoopsTracker.showToast('Player removed from roster', 'success');
                } else {
                    alert('Player removed from roster');
                }
                location.reload();
            } else {
                throw new Error(result.error || 'Failed to remove player');
            }
        } catch (error) {
            console.error('Error removing player:', error);
            if (typeof HoopsTracker !== 'undefined') {
                HoopsTracker.showToast('Error: ' + error.message, 'danger');
            } else {
                alert('Error: ' + error.message);
            }
        }
    }

    // Utility methods
    getRosterId() {
        // Try multiple methods to get roster ID
        
        // Method 1: From URL path
        const pathParts = window.location.pathname.split('/');
        const rosterIndex = pathParts.indexOf('roster');
        if (rosterIndex !== -1 && pathParts[rosterIndex + 1]) {
            const rosterId = parseInt(pathParts[rosterIndex + 1]);
            if (!isNaN(rosterId)) {
                return rosterId;
            }
        }
        
        // Method 2: From data attribute
        const rosterElement = document.querySelector('[data-roster-id]');
        if (rosterElement) {
            const rosterId = parseInt(rosterElement.getAttribute('data-roster-id'));
            if (!isNaN(rosterId)) {
                return rosterId;
            }
        }
        
        // Method 3: From meta tag (if you add one)
        const metaTag = document.querySelector('meta[name="roster-id"]');
        if (metaTag) {
            const rosterId = parseInt(metaTag.getAttribute('content'));
            if (!isNaN(rosterId)) {
                return rosterId;
            }
        }
        
        console.error('Could not determine roster ID from any method');
        return null;
    }

    isPlayerInRoster(playerId) {
        return this.currentRosterPlayers.has(parseInt(playerId));
    }
}

// Global functions for onclick handlers
function viewPlayer(playerId) {
    if (playerId && playerId > 0) {
        window.location.href = `/player/${playerId}`;
    }
}

function editRoster(rosterId) {
    if (typeof HoopsTracker !== 'undefined') {
        HoopsTracker.showToast('Edit functionality coming soon!', 'info');
    } else {
        alert('Edit functionality coming soon!');
    }
}

function shareRoster(rosterId) {
    if (typeof HoopsTracker !== 'undefined') {
        HoopsTracker.showToast('Share functionality coming soon!', 'info');
    } else {
        alert('Share functionality coming soon!');
    }
}

function deleteRoster(rosterId) {
    if (confirm('Are you sure you want to delete this roster? This action cannot be undone.')) {
        if (typeof HoopsTracker !== 'undefined') {
            HoopsTracker.showToast('Delete functionality coming soon!', 'info');
        } else {
            alert('Delete functionality coming soon!');
        }
    }
}

function removePlayer(event, rosterId, playerId) {
    if (window.rosterManager) {
        window.rosterManager.removePlayer(event, rosterId, playerId);
    }
}

// Initialize when DOM is ready
let rosterManager;
document.addEventListener('DOMContentLoaded', () => {
    rosterManager = new RosterDetailManager();
    // Make it globally accessible
    window.rosterManager = rosterManager;
});