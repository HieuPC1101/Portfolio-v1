"""
Application configuration using Pydantic Settings.
"""

from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    app_name: str = "Portfolio Optimization API"
    app_version: str = "1.0.0"
    debug: bool = False
    environment: str = "development"

    # API
    api_v1_prefix: str = "/api/v1"
    allowed_origins: str = "http://localhost:3000,http://localhost:8000"

    # Database (PostgreSQL)
    database_url: str = "postgresql://user:password@localhost:5432/portfolio_db"
    database_pool_size: int = 20
    database_max_overflow: int = 10

    # Supabase Configuration
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_service_role_key: str = ""  # For admin operations
    use_supabase: bool = False  # Toggle between local PostgreSQL and Supabase

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # JWT Authentication
    secret_key: str = "your-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7

    # Google Gemini API
    gemini_api_key: str = ""

    # Celery
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # Market Data
    vnstock_api_key: str = ""
    vnstock_cache_expire_seconds: int = 300
    default_market: str = "HOSE"

    # Notifications scheduler
    notification_scheduler_enabled: bool = True
    notification_scheduler_interval_seconds: int = 300
    notification_news_lookback_hours: int = 24

    # Logging
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="allow"
    )

    @property
    def cors_origins(self) -> List[str]:
        """Parse CORS origins from comma-separated string."""
        return [origin.strip() for origin in self.allowed_origins.split(",")]


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    This ensures we only read the .env file once.
    """
    return Settings()


# Convenience instance
settings = get_settings()
