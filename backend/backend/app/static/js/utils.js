// Utility functions for Hoops Tracker

// Debounce function for search inputs
function debounce(func, wait, immediate) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            timeout = null;
            if (!immediate) func(...args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func(...args);
    };
}

// Throttle function for scroll events
function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// Local storage helpers
const storage = {
    set(key, value) {
        try {
            localStorage.setItem(key, JSON.stringify(value));
        } catch (e) {
            console.warn('Failed to save to localStorage:', e);
        }
    },
    
    get(key, defaultValue = null) {
        try {
            const item = localStorage.getItem(key);
            return item ? JSON.parse(item) : defaultValue;
        } catch (e) {
            console.warn('Failed to read from localStorage:', e);
            return defaultValue;
        }
    },
    
    remove(key) {
        try {
            localStorage.removeItem(key);
        } catch (e) {
            console.warn('Failed to remove from localStorage:', e);
        }
    },
    
    clear() {
        try {
            localStorage.clear();
        } catch (e) {
            console.warn('Failed to clear localStorage:', e);
        }
    }
};

// Date utilities
const dateUtils = {
    formatDate(date, options = {}) {
        const defaultOptions = {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        };
        return new Intl.DateTimeFormat('en-US', { ...defaultOptions, ...options })
            .format(new Date(date));
    },
    
    formatTime(date) {
        return new Intl.DateTimeFormat('en-US', {
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
        }).format(new Date(date));
    },
    
    formatDateTime(date) {
        return `${this.formatDate(date)} at ${this.formatTime(date)}`;
    },
    
    daysAgo(date) {
        const now = new Date();
        const target = new Date(date);
        const diffTime = Math.abs(now - target);
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
        
        if (diffDays === 0) return 'Today';
        if (diffDays === 1) return 'Yesterday';
        return `${diffDays} days ago`;
    },
    
    isToday(date) {
        const today = new Date();
        const target = new Date(date);
        return today.toDateString() === target.toDateString();
    }
};

// Number formatting utilities
const numberUtils = {
    formatCurrency(amount, currency = 'USD') {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: currency
        }).format(amount);
    },
    
    formatPercentage(value, decimals = 1) {
        return `${(value * 100).toFixed(decimals)}%`;
    },
    
    formatNumber(value, decimals = 0) {
        return new Intl.NumberFormat('en-US', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        }).format(value);
    },
    
    formatCompact(value) {
        return new Intl.NumberFormat('en-US', {
            notation: 'compact',
            maximumFractionDigits: 1
        }).format(value);
    },
    
    formatOrdinal(value) {
        const suffixes = ['th', 'st', 'nd', 'rd'];
        const v = value % 100;
        return value + (suffixes[(v - 20) % 10] || suffixes[v] || suffixes[0]);
    }
};

// String utilities
const stringUtils = {
    capitalize(str) {
        return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
    },
    
    titleCase(str) {
        return str.replace(/\w\S*/g, (txt) => 
            txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
        );
    },
    
    truncate(str, length = 50, suffix = '...') {
        if (str.length <= length) return str;
        return str.substring(0, length) + suffix;
    },
    
    slugify(str) {
        return str
            .toLowerCase()
            .replace(/[^\w ]+/g, '')
            .replace(/ +/g, '-');
    },
    
    removeAccents(str) {
        return str.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    }
};

// Array utilities
const arrayUtils = {
    unique(arr) {
        return [...new Set(arr)];
    },
    
    groupBy(arr, key) {
        return arr.reduce((groups, item) => {
            const group = item[key];
            groups[group] = groups[group] || [];
            groups[group].push(item);
            return groups;
        }, {});
    },
    
    sortBy(arr, key, direction = 'asc') {
        return arr.sort((a, b) => {
            if (direction === 'desc') {
                return b[key] > a[key] ? 1 : -1;
            }
            return a[key] > b[key] ? 1 : -1;
        });
    },
    
    chunk(arr, size) {
        const chunks = [];
        for (let i = 0; i < arr.length; i += size) {
            chunks.push(arr.slice(i, i + size));
        }
        return chunks;
    },
    
    shuffle(arr) {
        const shuffled = [...arr];
        for (let i = shuffled.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }
        return shuffled;
    }
};

// DOM utilities
const domUtils = {
    createElement(tag, attributes = {}, children = []) {
        const element = document.createElement(tag);
        
        Object.entries(attributes).forEach(([key, value]) => {
            if (key === 'className') {
                element.className = value;
            } else if (key.startsWith('data-')) {
                element.setAttribute(key, value);
            } else {
                element[key] = value;
            }
        });
        
        children.forEach(child => {
            if (typeof child === 'string') {
                element.appendChild(document.createTextNode(child));
            } else {
                element.appendChild(child);
            }
        });
        
        return element;
    },
    
    addEventListeners(element, events) {
        Object.entries(events).forEach(([event, handler]) => {
            element.addEventListener(event, handler);
        });
    },
    
    toggleClass(element, className, force) {
        if (force !== undefined) {
            element.classList.toggle(className, force);
        } else {
            element.classList.toggle(className);
        }
    },
    
    getElementPosition(element) {
        const rect = element.getBoundingClientRect();
        return {
            top: rect.top + window.pageYOffset,
            left: rect.left + window.pageXOffset,
            width: rect.width,
            height: rect.height
        };
    },
    
    isInViewport(element) {
        const rect = element.getBoundingClientRect();
        return (
            rect.top >= 0 &&
            rect.left >= 0 &&
            rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
            rect.right <= (window.innerWidth || document.documentElement.clientWidth)
        );
    }
};

// URL utilities
const urlUtils = {
    getQueryParams() {
        return new URLSearchParams(window.location.search);
    },
    
    setQueryParam(key, value) {
        const url = new URL(window.location);
        url.searchParams.set(key, value);
        window.history.pushState({}, '', url);
    },
    
    removeQueryParam(key) {
        const url = new URL(window.location);
        url.searchParams.delete(key);
        window.history.pushState({}, '', url);
    },
    
    buildUrl(base, params = {}) {
        const url = new URL(base);
        Object.entries(params).forEach(([key, value]) => {
            url.searchParams.set(key, value);
        });
        return url.toString();
    }
};

// Validation utilities
const validators = {
    email(email) {
        const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return re.test(email);
    },
    
    phone(phone) {
        const re = /^\+?[\d\s\-\(\)]+$/;
        return re.test(phone);
    },
    
    password(password) {
        // At least 8 characters, 1 uppercase, 1 lowercase, 1 number
        const re = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[a-zA-Z\d@$!%*?&]{8,}$/;
        return re.test(password);
    },
    
    required(value) {
        return value !== null && value !== undefined && value.toString().trim() !== '';
    },
    
    minLength(value, min) {
        return value && value.length >= min;
    },
    
    maxLength(value, max) {
        return value && value.length <= max;
    },
    
    numeric(value) {
        return !isNaN(value) && !isNaN(parseFloat(value));
    },
    
    positiveNumber(value) {
        return this.numeric(value) && parseFloat(value) > 0;
    }
};

// Basketball-specific utilities
const basketballUtils = {
    formatStatLine(stats) {
        const { points = 0, rebounds = 0, assists = 0 } = stats;
        return `${points}/${rebounds}/${assists}`;
    },
    
    calculateFGPercentage(made, attempted) {
        if (attempted === 0) return 0;
        return (made / attempted) * 100;
    },
    
    calculatePER(stats) {
        // Simplified PER calculation
        const { points = 0, rebounds = 0, assists = 0, steals = 0, blocks = 0, 
                turnovers = 0, fouls = 0, minutes = 1 } = stats;
        
        const per = ((points + rebounds + assists + steals + blocks) - 
                    (turnovers + fouls)) / minutes * 36;
        return Math.max(0, per);
    },
    
    formatPosition(position) {
        const positions = {
            'PG': 'Point Guard',
            'SG': 'Shooting Guard', 
            'SF': 'Small Forward',
            'PF': 'Power Forward',
            'C': 'Center',
            'G': 'Guard',
            'F': 'Forward'
        };
        return positions[position] || position;
    },
    
    getPositionColor(position) {
        const colors = {
            'PG': 'text-blue-600',
            'SG': 'text-green-600',
            'SF': 'text-yellow-600',
            'PF': 'text-red-600',
            'C': 'text-purple-600',
            'G': 'text-blue-500',
            'F': 'text-orange-600'
        };
        return colors[position] || 'text-gray-600';
    }
};

// Export utilities to global scope
window.utils = {
    debounce,
    throttle,
    storage,
    dateUtils,
    numberUtils,
    stringUtils,
    arrayUtils,
    domUtils,
    urlUtils,
    validators,
    basketballUtils
};

// Console helper for development
if (window.location.hostname === 'localhost') {
    window.dev = {
        log: (...args) => console.log('🏀 Hoops Tracker:', ...args),
        warn: (...args) => console.warn('⚠️ Hoops Tracker:', ...args),
        error: (...args) => console.error('❌ Hoops Tracker:', ...args),
        utils: window.utils
    };
}