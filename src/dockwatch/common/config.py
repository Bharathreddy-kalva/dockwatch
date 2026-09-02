from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://dockwatch:dockwatch@localhost:5432/dockwatch"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    gbfs_station_status_url: str = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
    gbfs_station_info_url: str = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
    open_meteo_url: str = "https://api.open-meteo.com/v1/forecast"
    # Historical hourly weather (forecast endpoint above only covers recent/future
    # dates); used to backfill weather for a trip data month already in the past.
    open_meteo_archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "dockwatch"
    s3_secret_key: str = "dockwatch123"
    s3_bucket: str = "dockwatch-lake"

    class Config:
        env_file = ".env"


settings = Settings()
