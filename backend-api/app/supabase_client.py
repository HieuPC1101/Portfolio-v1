"""
Supabase client configuration and helper functions.
"""

from supabase import create_client, Client
from typing import Optional, Dict, Any, List
from functools import lru_cache
import logging

from app.config import settings

logger = logging.getLogger(__name__)


@lru_cache()
def get_supabase_client() -> Optional[Client]:
    """
    Get Supabase client instance (cached).
    Returns None if Supabase is not configured.
    """
    if not settings.use_supabase:
        logger.warning("Supabase is not enabled. Set USE_SUPABASE=True in .env")
        return None

    if not settings.supabase_url or not settings.supabase_key:
        logger.error("Supabase URL or KEY not configured in environment variables")
        return None

    try:
        client: Client = create_client(settings.supabase_url, settings.supabase_key)
        logger.info("Supabase client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None


@lru_cache()
def get_supabase_admin_client() -> Optional[Client]:
    """
    Get Supabase admin client with service role key (cached).
    Use this for admin operations that bypass Row Level Security (RLS).
    """
    if not settings.use_supabase:
        return None

    if not settings.supabase_url or not settings.supabase_service_role_key:
        logger.error("Supabase URL or Service Role Key not configured")
        return None

    try:
        client: Client = create_client(
            settings.supabase_url, settings.supabase_service_role_key
        )
        logger.info("Supabase admin client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Supabase admin client: {e}")
        return None


class SupabaseHelper:
    """Helper class for common Supabase operations."""

    def __init__(self, admin: bool = False):
        """
        Initialize Supabase helper.

        Args:
            admin: If True, use admin client with service role key
        """
        self.client = get_supabase_admin_client() if admin else get_supabase_client()
        if not self.client:
            raise ValueError("Supabase client not configured")

    def insert(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single record."""
        try:
            response = self.client.table(table).insert(data).execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"Error inserting into {table}: {e}")
            raise

    def insert_many(
        self, table: str, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Insert multiple records."""
        try:
            response = self.client.table(table).insert(data).execute()
            return response.data
        except Exception as e:
            logger.error(f"Error bulk inserting into {table}: {e}")
            raise

    def select(
        self,
        table: str,
        columns: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Select records with optional filters.

        Args:
            table: Table name
            columns: Columns to select (default: all)
            filters: Dict of column: value filters
            order_by: Column name to order by
            limit: Maximum number of records
        """
        try:
            query = self.client.table(table).select(columns)

            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)

            if order_by:
                query = query.order(order_by)

            if limit:
                query = query.limit(limit)

            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"Error selecting from {table}: {e}")
            raise

    def update(
        self, table: str, data: Dict[str, Any], filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Update records matching filters."""
        try:
            query = self.client.table(table).update(data)

            for key, value in filters.items():
                query = query.eq(key, value)

            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"Error updating {table}: {e}")
            raise

    def delete(self, table: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Delete records matching filters."""
        try:
            query = self.client.table(table).delete()

            for key, value in filters.items():
                query = query.eq(key, value)

            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"Error deleting from {table}: {e}")
            raise

    def upsert(
        self,
        table: str,
        data: Dict[str, Any] | List[Dict[str, Any]],
        on_conflict: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Upsert (insert or update) records.

        Args:
            table: Table name
            data: Single dict or list of dicts to upsert
            on_conflict: Column name(s) to use for conflict resolution
        """
        try:
            query = self.client.table(table).upsert(data)

            if on_conflict:
                query = query.on_conflict(on_conflict)

            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"Error upserting into {table}: {e}")
            raise

    def execute_rpc(
        self, function_name: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Execute a Supabase database function (RPC).

        Args:
            function_name: Name of the PostgreSQL function
            params: Parameters to pass to the function
        """
        try:
            response = self.client.rpc(function_name, params or {}).execute()
            return response.data
        except Exception as e:
            logger.error(f"Error executing RPC {function_name}: {e}")
            raise


# Convenience function for quick access
def supabase() -> Client:
    """Get Supabase client instance."""
    client = get_supabase_client()
    if not client:
        raise ValueError(
            "Supabase is not configured. Check your environment variables."
        )
    return client


def supabase_admin() -> Client:
    """Get Supabase admin client instance."""
    client = get_supabase_admin_client()
    if not client:
        raise ValueError("Supabase admin client is not configured.")
    return client
