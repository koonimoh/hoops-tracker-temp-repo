// Simple Search Functionality - No External Dependencies
class SimpleSearch {
    constructor() {
        this.searchCache = new Map();
        this.debounceTimer = null;
        this.minSearchLength = 2;
        this.debounceDelay = 300;
        
        this.init();
    }
    
    init() {
        this.setupSearchListeners();
    }
    
    setupSearchListeners() {
        // Find all search inputs
        const searchInputs = document.querySelectorAll('input[type="search"], input[data-search="true"], #player-search');
        
        searchInputs.forEach(input => {
            input.addEventListener('input', (e) => {
                this.handleSearchInput(e);
            });
            
            input.addEventListener('focus', (e) => {
                this.handleSearchFocus(e);
            });
            
            input.addEventListener('blur', (e) => {
                // Hide results after a short delay to allow clicks
                setTimeout(() => this.hideResults(e.target), 200);
            });
        });
        
        // Handle search form submissions
        const searchForms = document.querySelectorAll('form[data-search-form="true"], .search-form');
        searchForms.forEach(form => {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                const query = form.querySelector('input').value;
                if (query.trim()) {
                    this.performFullSearch(query);
                }
            });
        });
    }
    
    handleSearchInput(event) {
        const input = event.target;
        const query = input.value.trim();
        
        // Clear previous timer
        clearTimeout(this.debounceTimer);
        
        if (query.length < this.minSearchLength) {
            this.hideResults(input);
            return;
        }
        
        // Debounce the search
        this.debounceTimer = setTimeout(() => {
            this.performSearch(query, input);
        }, this.debounceDelay);
    }
    
    handleSearchFocus(event) {
        const input = event.target;
        const query = input.value.trim();
        
        if (query.length >= this.minSearchLength) {
            this.performSearch(query, input);
        } else {
            this.showPopularSearches(input);
        }
    }
    
    async performSearch(query, inputElement) {
        try {
            // Check cache first
            if (this.searchCache.has(query)) {
                this.displayResults(this.searchCache.get(query), inputElement);
                return;
            }
            
            // Show loading
            this.showLoading(inputElement);
            
            // Perform search
            const results = await this.searchPlayers(query);
            
            // Cache results
            this.searchCache.set(query, results);
            
            // Display results
            this.displayResults(results, inputElement);
            
        } catch (error) {
            console.error('Search error:', error);
            this.showError(inputElement);
        }
    }
    
    async searchPlayers(query) {
        try {
            const response = await fetch(`/api/search/players?q=${encodeURIComponent(query)}&limit=8`);
            
            if (!response.ok) {
                throw new Error('Search request failed');
            }
            
            const data = await response.json();
            return data.players || [];
            
        } catch (error) {
            console.error('Player search failed:', error);
            return [];
        }
    }
    
    async getSuggestions(query) {
        try {
            const response = await fetch(`/api/search/suggestions?q=${encodeURIComponent(query)}&limit=5`);
            
            if (!response.ok) {
                return [];
            }
            
            const data = await response.json();
            return data.suggestions || [];
            
        } catch (error) {
            console.error('Suggestions failed:', error);
            return [];
        }
    }
    
    displayResults(results, inputElement) {
        const container = this.getResultsContainer(inputElement);
        
        if (!results || results.length === 0) {
            container.innerHTML = `
                <div class="search-result-item no-results">
                    <div class="p-3 text-gray-500 text-center">No players found</div>
                </div>
            `;
        } else {
            const resultsHtml = results.map(player => this.createResultItem(player, inputElement.value)).join('');
            container.innerHTML = resultsHtml;
        }
        
        this.showResults(container);
    }
    
    createResultItem(player, query) {
        const highlightedName = this.highlightMatch(player.name, query);
        const teamInfo = player.teams ? player.teams.name : (player.team_name || '');
        
        return `
            <div class="search-result-item" onclick="window.searchManager.selectPlayer('${player.id}')">
                <div class="flex items-center p-3 hover:bg-gray-50 cursor-pointer">
                    <div class="flex-1">
                        <div class="font-medium text-gray-900">${highlightedName}</div>
                        ${teamInfo ? `<div class="text-sm text-gray-500">${teamInfo}</div>` : ''}
                        ${player.position ? `<div class="text-xs text-gray-400">${player.position}</div>` : ''}
                    </div>
                    <div class="text-gray-400">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                        </svg>
                    </div>
                </div>
            </div>
        `;
    }
    
    highlightMatch(text, query) {
        if (!query || query.length < 2) return text;
        
        // Simple highlighting - escape special regex characters
        const escapedQuery = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const regex = new RegExp(`(${escapedQuery})`, 'gi');
        
        return text.replace(regex, '<mark class="bg-yellow-200 font-medium">$1</mark>');
    }
    
    showPopularSearches(inputElement) {
        const popular = [
            { id: '1', name: 'LeBron James', team_name: 'Los Angeles Lakers', position: 'SF' },
            { id: '2', name: 'Stephen Curry', team_name: 'Golden State Warriors', position: 'PG' },
            { id: '3', name: 'Kevin Durant', team_name: 'Phoenix Suns', position: 'SF' }
        ];
        
        const container = this.getResultsContainer(inputElement);
        container.innerHTML = `
            <div class="p-2 border-b text-xs text-gray-500 font-medium uppercase tracking-wide">Popular Searches</div>
            ${popular.map(player => this.createResultItem(player, '')).join('')}
        `;
        
        this.showResults(container);
    }
    
    showLoading(inputElement) {
        const container = this.getResultsContainer(inputElement);
        container.innerHTML = `
            <div class="search-result-item loading">
                <div class="p-3 text-center text-gray-500">
                    <div class="inline-block animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
                    <span class="ml-2">Searching...</span>
                </div>
            </div>
        `;
        this.showResults(container);
    }
    
    showError(inputElement) {
        const container = this.getResultsContainer(inputElement);
        container.innerHTML = `
            <div class="search-result-item error">
                <div class="p-3 text-center text-red-500">Search failed. Please try again.</div>
            </div>
        `;
        this.showResults(container);
    }
    
    getResultsContainer(inputElement) {
        let container = inputElement.parentElement.querySelector('.search-results');
        
        if (!container) {
            container = document.createElement('div');
            container.className = 'search-results absolute z-50 w-full bg-white border border-gray-200 rounded-lg shadow-lg mt-1 max-h-80 overflow-y-auto hidden';
            
            // Position relative to input
            const parent = inputElement.parentElement;
            if (parent.style.position !== 'relative') {
                parent.style.position = 'relative';
            }
            
            parent.appendChild(container);
        }
        
        return container;
    }
    
    showResults(container) {
        container.classList.remove('hidden');
    }
    
    hideResults(inputElement) {
        const container = inputElement.parentElement.querySelector('.search-results');
        if (container) {
            container.classList.add('hidden');
        }
    }
    
    selectPlayer(playerId) {
        // Navigate to player page
        window.location.href = `/players/${playerId}`;
    }
    
    performFullSearch(query) {
        // Navigate to search results page
        window.location.href = `/players?search=${encodeURIComponent(query)}`;
    }
    
    // Utility method to clear cache
    clearCache() {
        this.searchCache.clear();
    }
}

// Global functions for template use
window.selectPlayer = function(playerId) {
    if (window.searchManager) {
        window.searchManager.selectPlayer(playerId);
    }
};

window.clearSearchCache = function() {
    if (window.searchManager) {
        window.searchManager.clearCache();
    }
};

// Initialize search when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    window.searchManager = new SimpleSearch();
    console.log('Search functionality initialized');
});

// Close search results when clicking outside
document.addEventListener('click', function(event) {
    const searchResults = document.querySelectorAll('.search-results');
    searchResults.forEach(container => {
        if (!container.contains(event.target) && !container.previousElementSibling.contains(event.target)) {
            container.classList.add('hidden');
        }
    });
});