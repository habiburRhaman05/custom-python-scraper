"""Settings loaded from environment / .env (see .env.example)."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # database
    database_url: str = "sqlite:///./maisondubai.db"

    # admin auth
    admin_api_key: str = ""

    # api. Kept as a plain string: pydantic-settings JSON-decodes list[str]
    # fields straight from the environment, so "a,b" would be a parse error.
    cors_origins: str = "http://localhost:3000"
    api_title: str = "maisondubai data API"

    # scrape tuning
    scrape_output_dir: str = "output"
    scrape_cache_dir: str = ".cache_maisondubai"
    scrape_workers: int = 6
    scrape_delay: float = 0.3
    scrape_revalidate: bool = True

    # safety rails
    guard_enabled: bool = True
    guard_min_count_ratio: float = 0.8
    guard_max_coverage_drop: float = 30.0
    guard_max_delist_ratio: float = 0.2

    # GoHighLevel media mirroring (app/media_migration.py) -- optional, only
    # needed if you run `python -m app.cli migrate-media`
    ghl_pit_token: str = ""
    ghl_location_id: str = ""
    ghl_parent_folder_id: str = ""  # optional: upload into a specific media folder
    ghl_rate_limit_per_sec: float = 4.0

    @property
    def cors_origin_list(self) -> list[str]:
        """CORS_ORIGINS as a list. `*` is passed through as the wildcard."""
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
