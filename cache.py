"""
Simple in-memory caching layer for PathPilot FHIR API
Optimized for static test data with long TTLs
"""

import hashlib
import time
from typing import Any, Optional, Callable, Dict, Union
from functools import wraps
from datetime import datetime, timedelta

class InMemoryCache:
    """Simple in-memory cache with TTL support"""

    def __init__(self, default_ttl: Optional[int] = None, max_size: int = 1000):
        """
        Initialize cache

        Args:
            default_ttl: Default time-to-live in seconds (None = never expire)
            max_size: Maximum number of items to cache
        """
        self.cache: Dict[str, tuple[Any, Optional[float]]] = {}
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def _is_expired(self, timestamp: Optional[float]) -> bool:
        """Check if cached item has expired"""
        if timestamp is None:
            return False  # Never expires
        return time.time() > timestamp

    def _evict_lru(self):
        """Evict least recently used items if cache is full"""
        if len(self.cache) >= self.max_size:
            # Remove oldest item (simple FIFO for now)
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if key in self.cache:
            value, expiry = self.cache[key]
            if not self._is_expired(expiry):
                self.hits += 1
                return value
            else:
                # Remove expired entry
                del self.cache[key]

        self.misses += 1
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache"""
        ttl = ttl if ttl is not None else self.default_ttl
        expiry = (time.time() + ttl) if ttl else None  # None = never expires

        self._evict_lru()
        self.cache[key] = (value, expiry)

    def clear(self, pattern: Optional[str] = None) -> int:
        """Clear cache entries matching pattern or all if pattern is None"""
        if pattern is None:
            count = len(self.cache)
            self.cache.clear()
            return count

        keys_to_delete = [k for k in self.cache.keys() if pattern in k]
        for key in keys_to_delete:
            del self.cache[key]
        return len(keys_to_delete)

    def get_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0

        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.1f}%",
            "default_ttl_hours": (self.default_ttl / 3600) if self.default_ttl else "Never expires"
        }

# Global cache instances for different data types - NO EXPIRATION for static data
patient_cache = InMemoryCache(default_ttl=None, max_size=500)  # Never expires
resource_cache = InMemoryCache(default_ttl=None, max_size=1000)  # Never expires
bundle_cache = InMemoryCache(default_ttl=None, max_size=200)  # Never expires

def generate_cache_key(*args, **kwargs) -> str:
    """Generate cache key from function arguments"""
    key_parts = [str(arg) for arg in args]
    key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
    key_string = ":".join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()

def cache_result(cache: InMemoryCache, ttl: Optional[int] = None):
    """
    Decorator to cache function results

    Args:
        cache: The cache instance to use
        ttl: Time-to-live in seconds (uses cache default if None)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = f"{func.__name__}:{generate_cache_key(*args, **kwargs)}"

            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # Call function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)

            return result

        # Add cache control methods to the wrapper
        wrapper.cache_clear = lambda: cache.clear(func.__name__)
        wrapper.cache_stats = lambda: cache.get_stats()

        return wrapper
    return decorator

def cache_async_result(cache: InMemoryCache, ttl: Optional[int] = None):
    """
    Decorator to cache async function results

    Args:
        cache: The cache instance to use
        ttl: Time-to-live in seconds (uses cache default if None)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = f"{func.__name__}:{generate_cache_key(*args, **kwargs)}"

            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # Call function and cache result
            result = await func(*args, **kwargs)
            cache.set(cache_key, result, ttl)

            return result

        # Add cache control methods to the wrapper
        wrapper.cache_clear = lambda: cache.clear(func.__name__)
        wrapper.cache_stats = lambda: cache.get_stats()

        return wrapper
    return decorator

# Convenience decorators for common use cases
def cache_patient_data(ttl: Optional[int] = None):
    """Cache patient-related data (never expires by default)"""
    return cache_async_result(patient_cache, ttl)

def cache_fhir_resource(ttl: Optional[int] = None):
    """Cache FHIR resources (never expires by default)"""
    return cache_result(resource_cache, ttl)

def cache_fhir_bundle(ttl: Optional[int] = None):
    """Cache FHIR bundle responses (never expires by default)"""
    return cache_result(bundle_cache, ttl)

def get_cache_statistics() -> dict:
    """Get statistics for all caches"""
    return {
        "patient_cache": patient_cache.get_stats(),
        "resource_cache": resource_cache.get_stats(),
        "bundle_cache": bundle_cache.get_stats(),
        "timestamp": datetime.now().isoformat()
    }

def clear_all_caches() -> dict:
    """Clear all cache instances"""
    return {
        "patient_cache_cleared": patient_cache.clear(),
        "resource_cache_cleared": resource_cache.clear(),
        "bundle_cache_cleared": bundle_cache.clear(),
        "timestamp": datetime.now().isoformat()
    }