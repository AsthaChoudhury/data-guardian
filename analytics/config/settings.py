import yaml
from pathlib import Path
from pydantic import BaseSettings


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

    BATCH_SCHEDULE: str = "daily"
    API_HOST: str = "localhost"
    API_PORT: int = 8080
    LOG_LEVEL: str = "INFO"
    DQ_RULES_PATH: str = "config/dq_rules.yaml"

    @property
    def DQ_RULES(self):
        with open(self.DQ_RULES_PATH, "r") as f:
            return yaml.safe_load(f)

    class Config:
        env_file = ".env"


settings = Settings()
