from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "trips"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "trips.raw"
    spark_checkpoint_dir: str = "/tmp/spark-checkpoints/trips"
    csv_chunk_size: int = 500

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def sqlalchemy_database_uri(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def psycopg_database_uri(self) -> str:
        return (
            f"dbname={self.postgres_db} user={self.postgres_user} password={self.postgres_password} "
            f"host={self.postgres_host} port={self.postgres_port}"
        )


settings = Settings()
