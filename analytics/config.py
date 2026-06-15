import os
from dotenv import load_dotenv

load_dotenv()


class Config:

    # Kafka
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    KAFKA_RAW_TOPIC = "patient_data"
    KAFKA_ISSUES_TOPIC = "dq_issues"

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = 0

    # Spark
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    CHECKPOINT_DIR = "/tmp/dg_checkpoint"

    # Batch processing
    BATCH_SCHEDULE = "daily"
    DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "s3://data-guardian/")

    # DQ Rules
    DQ_RULES = {
        "completeness": {
            "required_fields": ["patient_id", "age", "bp_systolic", "temperature"],
            "max_null_pct": 5
        },
        "validity": {
            "age": {"min": 0, "max": 150},
            "bp_systolic": {"min": 50, "max": 250},
            "temperature": {"min": 35.0, "max": 45.0}
        },
        "anomaly_detection": {
            "z_score_threshold": 3,
            "window_size": 1000
        }
    }


config = Config()
