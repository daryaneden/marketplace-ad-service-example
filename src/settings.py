from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AliasChoices, Field, field_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5434/ads_db",
        validation_alias=AliasChoices(
            "POSTGRES_CONNECTION_STRING",
            "DATABASE_URL",
            "database_url",
        ),
    )
    jwt_secret: str = "change-me"
    jwt_algorithm: str = "HS256"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_ads: str = "ads"
    auth_service_url: str = "http://localhost:8000"

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if isinstance(value, str):
            if value.startswith("postgres://"):
                return value.replace(
                    "postgres://",
                    "postgresql+asyncpg://",
                    1,
                )

            if value.startswith("postgresql://"):
                return value.replace(
                    "postgresql://",
                    "postgresql+asyncpg://",
                    1,
                )

        return value

