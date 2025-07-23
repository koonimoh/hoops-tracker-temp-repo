import time
import asyncio
from functools import wraps
from typing import Callable, Any
from app.core.logging import logger
from app.core.cache import cache as cached
import threading
from collections import defaultdict

# Rate limiting storage
_rate_limit_calls = defaultdict(list)
_rate_limit_lock = threading.Lock()

def rate_limit(calls: int, period: int):
    """Rate limiting decorator"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            func_key = f"{func.__module__}.{func.__name__}"
            
            with _rate_limit_lock:
                # Clean old entries
                _rate_limit_calls[func_key] = [
                    call_time for call_time in _rate_limit_calls[func_key]
                    if now - call_time < period
                ]
                
                # Check if we've exceeded the rate limit
                if len(_rate_limit_calls[func_key]) >= calls:
                    sleep_time = period - (now - _rate_limit_calls[func_key][0])
                    if sleep_time > 0:
                        logger.warning(f"Rate limit exceeded for {func_key}, sleeping {sleep_time:.2f}s")
                        time.sleep(sleep_time)
                
                # Record this call
                _rate_limit_calls[func_key].append(now)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

def retry_on_failure(max_attempts: int = 3, delay: int = 1, backoff: float = 2.0):
    """Retry decorator with exponential backoff"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            current_delay = delay
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    
                    logger.warning(f"Attempt {attempt} failed for {func.__name__}: {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    attempt += 1
                    current_delay *= backoff
            
        return wrapper
    return decorator

def async_route(func: Callable) -> Callable:
    """Decorator to run Flask route in thread pool for async operations"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # This will be used with Flask 2.0+ async routes
        return asyncio.run(func(*args, **kwargs))
    return wrapper

def timed_cache(timeout: int):
    """Simple timed cache decorator"""
    def decorator(func: Callable) -> Callable:
        cache_data = {}
        cache_timestamps = {}
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(sorted(kwargs.items()))
            now = time.time()
            
            # Check if we have cached data that's still valid
            if (key in cache_data and 
                key in cache_timestamps and 
                now - cache_timestamps[key] < timeout):
                return cache_data[key]
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache_data[key] = result
            cache_timestamps[key] = now
            
            return result
        return wrapper
    return decorator

def thread_safe(func: Callable) -> Callable:
    """Thread safety decorator using locks"""
    lock = threading.Lock()
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)
    return wrapper

def performance_monitor(func: Callable) -> Callable:
    """Performance monitoring decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Function {func.__name__} executed in {execution_time:.4f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Function {func.__name__} failed after {execution_time:.4f}s: {e}")
            raise
    return wrapper