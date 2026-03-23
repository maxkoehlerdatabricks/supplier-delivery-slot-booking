import asyncio
import asyncpg
import logging
import subprocess
import json
import os
from server.config import get_config

logger = logging.getLogger(__name__)


class LakebasePool:
    """Async connection pool for Lakebase with OAuth token refresh."""

    def __init__(self):
        self._pool: asyncpg.Pool | None = None
        self._config = get_config()
        self._host: str | None = None
        self._email: str | None = None

    async def initialize(self):
        """Initialize the connection pool."""
        self._host = self._config.get_db_host()
        token, self._email = self._config.get_db_credentials()

        logger.info(f"Connecting to Lakebase at {self._host} as {self._email}")

        self._pool = await asyncpg.create_pool(
            host=self._host,
            port=5432,
            database=self._config.db_name,
            user=self._email,
            password=token,
            ssl="require",
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        logger.info("Lakebase connection pool initialized")

    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Connection pool closed")

    async def refresh_pool(self):
        """Refresh the connection pool with a new token."""
        logger.info("Refreshing Lakebase connection pool...")
        if self._pool:
            await self._pool.close()
        await self.initialize()

    async def fetch(self, query: str, *args):
        """Execute a query and return all rows."""
        try:
            async with self._pool.acquire() as conn:
                return await conn.fetch(query, *args)
        except asyncpg.InvalidAuthorizationSpecificationError:
            await self.refresh_pool()
            async with self._pool.acquire() as conn:
                return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Execute a query and return a single row."""
        try:
            async with self._pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        except asyncpg.InvalidAuthorizationSpecificationError:
            await self.refresh_pool()
            async with self._pool.acquire() as conn:
                return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args):
        """Execute a query without returning results."""
        try:
            async with self._pool.acquire() as conn:
                return await conn.execute(query, *args)
        except asyncpg.InvalidAuthorizationSpecificationError:
            await self.refresh_pool()
            async with self._pool.acquire() as conn:
                return await conn.execute(query, *args)


db_pool = LakebasePool()
