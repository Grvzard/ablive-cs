from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, env_file=".env")

    workers_num: int
    server_url: str
    server_api_key: str
    machine_id: str
    add_room_interval: float
    packer1_mysql_dsn: str  # MysqlDsn
