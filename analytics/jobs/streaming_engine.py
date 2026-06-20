from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, struct, to_json, to_timestamp, when, current_timestamp, lit,
    abs, avg, stddev, lag, count, concat_ws, window
)
import json
import redis
from datetime import datetime
from config.settings import settings
from constants.redis_keys import RedisKeys
from core.spark_session import create_spark_session
from dq.calculate_quality_score import calculate_quality_score
from dq.check_validity import check_validity
from dq.detect_anomalies import detect_anomalies
from dq.completeness import check_completeness
from schema.patient_schema import PATIENT_SCHEMA


class DataQualityStreamingEngine:
    def __init__(self):

        print("[1/7] Creating Spark Session...")
        self.spark = create_spark_session()

        self.spark.sparkContext.setLogLevel("WARN")
        print("    Spark session created\n")
        print("[2/7] Connecting to Redis...")
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.redis_client.ping()
            print("    Redis connected\n")
        except Exception as e:
            print(
                f"  Redis unavailable: {e} (will continue without caching)\n")
            self.redis_client = None

        print("[3/7] Defining data schema...")
        self.schema = PATIENT_SCHEMA
        print("    Schema defined\n")

        print("[4/7] Validating DQ rules...")
        self.rules = settings.DQ_RULES
        print(f"    Loaded {len(self.rules)} rule categories\n")

    def create_stream_input(self):
        """
        Read from Kafka topic "patient_data"
        Returns: Streaming DataFrame
        """

        print("[5/7] Connecting to Kafka...")
        print(f"   Broker: {settings.KAFKA_BROKERS}")
        print(f"   Topic: {settings.KAFKA_RAW_TOPIC}")

        df_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
            .option("subscribe", settings.KAFKA_RAW_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("    Kafka connection established\n")

        return df_stream

    def parse_json(self, df_raw):

        df_parsed = df_raw.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        df_parsed = df_parsed.withColumn("ingestion_time", current_timestamp())
        df_parsed = df_parsed.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
        )

        return df_parsed

    def extract_issues(self, df_with_quality):

        issues = df_with_quality.filter(col("has_issues") == True) \
            .select(
                col("patient_id"),
                col("hospital"),
                col("age"),
                col("bp_systolic"),
                col("temperature"),
                col("quality_score"),
                col("completeness_score"),
                col("age_valid"),
                col("bp_valid"),
                col("temp_valid"),
                col("age_anomaly"),
                col("bp_anomaly"),
                col("ingestion_time").alias("detected_at")
        )

        return issues

    def output_to_console(self, df_issues):

        query = df_issues.writeStream \
            .format("console") \
            .option("truncate", False) \
            .option("checkpointLocation", f"{settings.SPARK_CHECKPOINT_DIR}/console") \
            .start()

        return query

    def output_to_kafka(self, df_issues):
        df_kafka_format = df_issues.select(
            to_json(struct("*")).alias("value")
        )

        query = df_kafka_format.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
            .option("topic", settings.KAFKA_ISSUES_TOPIC) \
            .option("checkpointLocation", f"{settings.SPARK_CHECKPOINT_DIR}/kafka") \
            .start()

        return query

    def output_to_parquet(self, df):
        query = df.writeStream \
            .format("parquet") \
            .trigger(processingTime="1 minute")\
            .option(
                "path",
                settings.DATA_LAKE_PATH
            ) \
            .option(
                "checkpointLocation",
                f"{settings.SPARK_CHECKPOINT_DIR}/parquet"
            ) \
            .start()

        return query

    def output_to_redis(self, batch_df, batch_id):

        if self.redis_client is None:
            return

        try:
            issue_count = batch_df.count()

            if issue_count > 0:
                hospital_issues = batch_df.groupBy(
                    "hospital").count().collect()
                avg_quality = batch_df.agg(
                    avg("quality_score")
                ).collect()[0][0]

                hospital_breakdown = {}
                for row in hospital_issues:
                    hospital_breakdown[row['hospital']] = row['count']
                cache_data = {
                    "current_issues": issue_count,
                    "hospital_breakdown": hospital_breakdown,
                    "timestamp": datetime.now().isoformat(),
                    "quality_metrics": {
                        "avg_quality_score": float(avg_quality)
                    }
                }

                self.redis_client.setex(
                    RedisKeys.CURRENT_METRICS,
                    300,
                    json.dumps(cache_data)
                )
        except Exception as e:
            print(f"⚠ Redis cache error: {e}")

    def run(self):
        """Execute the complete streaming pipeline"""

        try:
            print("[6/7] Building streaming pipeline...\n")
            df_raw = self.create_stream_input()
            df_parsed = self.parse_json(df_raw)
            df_parsed.printSchema()
            print(type(self.rules))
            print(self.rules)
            df_completeness = check_completeness(
                df_parsed,
                self.rules
            )
            df_validity = check_validity(df_completeness, self.rules)
            df_anomalies = detect_anomalies(df_validity, self.rules)

            df_with_quality = calculate_quality_score(df_anomalies)

            df_issues = self.extract_issues(df_with_quality)
            query_parquet = self.output_to_parquet(
                df_with_quality
            )

            query_console = self.output_to_console(df_issues)

            query_kafka = self.output_to_kafka(df_issues)

            query_redis = df_with_quality.writeStream \
                .foreachBatch(self.output_to_redis) \
                .option(
                    "checkpointLocation",
                    f"{settings.SPARK_CHECKPOINT_DIR}/redis"
                ) \
                .start()

            print("[7/7] Streaming pipeline active!\n")
            print("=" * 80)
            print(" Listening for incoming data...")
            print(f"  Input: Kafka topic '{settings.KAFKA_RAW_TOPIC}'")
            print(
                f"  Output: Console + Kafka topic '{settings.KAFKA_ISSUES_TOPIC}'")
            self.spark.streams.awaitAnyTermination()

        except KeyboardInterrupt:
            print("\n\n Streaming stopped by user")
        except Exception as e:
            print(f"\nError: {e}")
            raise
        finally:
            if self.redis_client:
                self.redis_client.close()
            self.spark.stop()


if __name__ == "__main__":
    engine = DataQualityStreamingEngine()
    engine.run()
