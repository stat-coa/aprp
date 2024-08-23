from django.core.cache import cache as base_cache
from django_redis import get_redis_connection


class RedisCache:
    """
    Redis cache class to handle cache operations using Django cache and Redis
    """
    def __init__(self):
        self.cache = base_cache

        # get redis connection
        self.redis = get_redis_connection("default")

    def get(self, key:str):
        return self.cache.get(key)

    def set(self, key:str, value, timeout=None):
        self.cache.set(key, value, timeout)

    def delete(self, key:str):
        self.cache.delete(key)

    def delete_keys(self, keys:list):
        # delete multiple keys
        self.cache.delete_many(keys)

    def delete_keys_with_pattern(self, pattern:str):
        """
        Delete keys with specific pattern
        """
        cursor = '0'

        while cursor != 0:
            # scan keys with pattern
            cursor, keys = self.redis.scan(cursor=cursor, match=pattern)

            if keys:
                # remove prefix from keys because cache keys do not need to prefix to delete
                cleaned_keys = [key.decode().lstrip(':1:') for key in keys]
                self.delete_keys(cleaned_keys)
