from pyspark.sql import SparkSession
from analytics.config import settings


def create_spark_session():

    return (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config(
            "spark.sql.streaming.checkpointLocation",
            settings.SPARK_CHECKPOINT_DIR
        )
        .getOrCreate()
    )
