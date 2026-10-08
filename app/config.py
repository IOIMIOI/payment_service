from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: str = Field(default="dev-api-key", min_length=1)
    provider_url: str = "http://localhost:8001"
    provider_api_key: str = Field(default="dev-provider-key", min_length=1)
    provider_delay_min: float = Field(default=2, ge=0)
    provider_delay_max: float = Field(default=5, ge=0)
    outbox_poll_interval: float = Field(default=0.5, gt=0)

    @model_validator(mode="after")
    def check_delays(self):
        if self.provider_delay_max < self.provider_delay_min:
            raise ValueError("provider_delay_max must be >= provider_delay_min")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
