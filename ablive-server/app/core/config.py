
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', case_sensitive=False)

    MONGO_URI: str
    API_KEY: str
    API_PREFIX: str = ""


settings = Settings()  # type: ignore
