from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    KAFKA_BROKERS: str
    KAFKA_RAW_TOPIC: str
    KAFKA_ISSUES_TOPIC: str

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int

    SPARK_MASTER: str
    SPARK_APP_NAME: str
    SPARK_CHECKPOINT_DIR: str

    DATA_LAKE_PATH: str

    class Config:
        env_file = ".env"


settings = Settings()
