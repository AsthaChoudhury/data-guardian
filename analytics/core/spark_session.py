from pyspark.sql import SparkSession
from config.settings import settings


def create_spark_session():
    return (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
        )
        .config(
            "spark.sql.streaming.checkpointLocation",
            settings.SPARK_CHECKPOINT_DIR
        )
        .getOrCreate()
    )
