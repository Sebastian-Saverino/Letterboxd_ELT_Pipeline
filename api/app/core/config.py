from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    postgres_meta_host: str = Field(default="postgres-meta", alias="POSTGRES_META_HOST")
    postgres_meta_port: int = Field(default=5432, alias="POSTGRES_META_PORT")
    postgres_meta_user: str = Field(alias="POSTGRES_META_USER")
    postgres_meta_password: str = Field(alias="POSTGRES_META_PASSWORD")
    postgres_meta_db: str = Field(alias="POSTGRES_META_DB")

    @property
    def metadata_database_url(self) -> str:
        return (
            "postgresql+psycopg2://"
            f"{self.postgres_meta_user}:{self.postgres_meta_password}"
            f"@{self.postgres_meta_host}:{self.postgres_meta_port}/{self.postgres_meta_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
