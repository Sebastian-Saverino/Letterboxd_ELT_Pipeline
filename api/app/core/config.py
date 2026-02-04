from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field
from sqlalchemy.engine import URL

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Metadata DB
    META_DB_HOST: str = "postgres_meta"
    META_DB_PORT: int = 5432
    META_DB_NAME: str = "metadata"
    META_DB_USER: str = "meta_user"
    META_DB_PASSWORD: SecretStr = Field(default=SecretStr("password"))

    # MinIO (S3)
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "admin"
    MINIO_SECRET_KEY: str = "password"
    MINIO_BUCKET_RAW: str = "raw"
    MINIO_SECURE: bool = False

    def meta_db_url(self) -> str:
        pw = self.META_DB_PASSWORD.get_secret_value()
        return str(
            URL.create(
                "postgresql+psycopg2",
                username=self.META_DB_USER,
                password=pw,
                host=self.META_DB_HOST,
                port=self.META_DB_PORT,
                database=self.META_DB_NAME,
            )
        )

settings = Settings()
