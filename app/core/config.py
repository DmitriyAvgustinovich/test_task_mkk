from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: SecretStr = Field(min_length=1)
    retry_base_delay_seconds: float = Field(default=2, gt=0)
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1, le=1000)
    webhook_timeout_seconds: float = Field(default=10, gt=0)
    gateway_min_delay_seconds: float = Field(default=2, ge=0)
    gateway_max_delay_seconds: float = Field(default=5, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_delays(self) -> "Settings":
        if self.gateway_max_delay_seconds < self.gateway_min_delay_seconds:
            raise ValueError("gateway_max_delay_seconds must be >= gateway_min_delay_seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
