from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://dockwatch:dockwatch@localhost:5432/dockwatch"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    gbfs_station_status_url: str = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
    gbfs_station_info_url: str = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

    class Config:
        env_file = ".env"


settings = Settings()
