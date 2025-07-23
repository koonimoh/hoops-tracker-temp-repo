// Advanced search functionality using Fuse.js
class SearchManager {
    constructor() {
        this.fuseOptions = {
            includeScore: true,
            threshold: 0.4,
            location: 0,
            distance: 100,
            maxPatternLength: 32,
            minMatchCharLength: 2,
            keys: [
                { name: 'name', weight: 0.7 },
                { name: 'team_name', weight: 0.2 },
                { name: 'position', weight: 0.1 }
            ]
        };
        
        this.playerFuse = null;
        this.searchCache = new Map();
        this.debounceTimer = null;
        
        this.initializeSearch();
    }
    
    async initializeSearch() {
        try {
            // Load initial player data for client-side fuzzy search
            const response = await fetch('/api/players/all');
            const players = await response.json();
            
            this.playerFuse = new Fuse(players, this.fuseOptions);
            
            // Setup search input listeners
            this.setupSearchListeners();
            
        } catch (error) {
            console.error('Failed to initialize search:', error);
            // Fallback to server-side search only
        }
    }
    
    setupSearchListeners() {
        const searchInputs = document.querySelectorAll('[data-search="true"]');
        
        searchInputs.forEach(input => {
            input.addEventListener('input', (e) => {
                clearTimeout(this.debounceTimer);
                this.debounceTimer = setTimeout(() => {
                    this.performSearch(e.target.value, e.target);
                }, 300);
            });
            
            input.addEventListener('focus', (e) => {
                this.showSearchSuggestions(e.target);
            });
        });
    }
    
    async performSearch(query, inputElement) {
        if (query.length < 2) {
            this.hideSearchResults(inputElement);
            return;
        }
        
        // Check cache first
        if (this.searchCache.has(query)) {
            this.displaySearchResults(this.searchCache.get(query), inputElement);
            return;
        }
        
        try {
            // Try client-side fuzzy search first
            let results = [];
            
            if (this.playerFuse && query.length >= 2) {
                const fuseResults = this.playerFuse.search(query);
                results = fuseResults.map(result => ({
                    ...result.item,
                    score: result.score
                }));
            }
            
            // If client-side search yields few results, supplement with server-side
            if (results.length < 5) {
                const serverResults = await this.serverSearch(query);
                results = this.mergeResults(results, serverResults);
            }
            
            // Cache results
            this.searchCache.set(query, results);
            
            // Display results
            this.displaySearchResults(results, inputElement);
            
        } catch (error) {
            console.error('Search error:', error);
            // Fallback to server search
            try {
                const serverResults = await this.serverSearch(query);
                this.displaySearchResults(serverResults, inputElement);
            } catch (fallbackError) {
                console.error('Fallback search failed:', fallbackError);
            }
        }
    }
    
    async serverSearch(query) {
        const response = await fetch(`/api/search/players?q=${encodeURIComponent(query)}`);
        if (!response.ok) throw new Error('Server search failed');
        return await response.json();
    }
    
    mergeResults(clientResults, serverResults) {
        const merged = [...clientResults];
        const clientIds = new Set(clientResults.map(r => r.id));
        
        // Add server results that aren't already in client results
        serverResults.forEach(result => {
            if (!clientIds.has(result.id)) {
                merged.push(result);
            }
        });
        
        // Sort by relevance (score or name)
        return merged.sort((a, b) => {
            if (a.score && b.score) return a.score - b.score;
            return a.name.localeCompare(b.name);
        });
    }
    
    displaySearchResults(results, inputElement) {
        const resultsContainer = this.getOrCreateResultsContainer(inputElement);
        
        if (results.length === 0) {
            resultsContainer.innerHTML = '<div class="p-2 text-gray-500">No results found</div>';
            resultsContainer.classList.remove('hidden');
            return;
        }
        
        const resultsHtml = results.map(result => `
            <div class="search-result-item p-2 hover:bg-gray-100 cursor-pointer flex items-center"
                 data-player-id="${result.id}"
                 onclick="window.searchManager.selectResult('${result.id}', '${result.name}')">
                <div class="flex-1">
                    <div class="font-medium">${this.highlightMatch(result.name, inputElement.value)}</div>
                    ${result.team_name ? `<div class="text-sm text-gray-500">${result.team_name}</div>` : ''}
                </div>
                ${result.score ? `<div class="text-xs text-gray-400">${(result.score * 100).toFixed(0)}%</div>` : ''}
            </div>
        `).join('');
        
        resultsContainer.innerHTML = resultsHtml;
        resultsContainer.classList.remove('hidden');
    }
    
    highlightMatch(text, query) {
        if (!query) return text;
        
        const regex = new RegExp(`(${query})`, 'gi');
        return text.replace(regex, '<mark class="bg-yellow-200">$1</mark>');
    }
    
    getOrCreateResultsContainer(inputElement) {
        let container = inputElement.parentElement.querySelector('.search-results');
        
        if (!container) {
            container = document.createElement('div');
            container.className = 'search-results absolute z-10 w-full bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto hidden';
            container.style.top = `${inputElement.offsetHeight}px`;
            inputElement.parentElement.appendChild(container);
        }
        
        return container;
    }
    
    selectResult(playerId, playerName) {
        // Navigate to player detail page
        window.location.href = `/players/${playerId}`;
    }
    
    hideSearchResults(inputElement) {
        const container = inputElement.parentElement.querySelector('.search-results');
        if (container) {
            container.classList.add('hidden');
        }
    }
    
    showSearchSuggestions(inputElement) {
        // Could show recent searches or popular players
        if (inputElement.value.length === 0) {
            // Show popular searches or recent searches
            this.displayPopularSearches(inputElement);
        }
    }
    
    displayPopularSearches(inputElement) {
        const popular = [
            { id: 'lebron', name: 'LeBron James', team_name: 'Los Angeles Lakers' },
            { id: 'curry', name: 'Stephen Curry', team_name: 'Golden State Warriors' },
            { id: 'durant', name: 'Kevin Durant', team_name: 'Phoenix Suns' }
        ];
        
        this.displaySearchResults(popular, inputElement);
    }
}

// Initialize search manager when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.searchManager = new SearchManager();
});

// Export for use in other scripts
window.SearchManager = SearchManager;