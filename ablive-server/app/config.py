
from pydantic import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str
    API_KEY: str
    API_PREFIX: str = ""

    class Config:
        env_file = '.env'
        case_sensitive = True


settings = Settings()
