from .redis_cache import RedisCache

redis_instance = RedisCache()

__all__ = ['redis_instance']
