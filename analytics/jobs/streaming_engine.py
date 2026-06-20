from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, struct, to_json, when, current_timestamp, lit,
    abs, avg, stddev, lag, count, concat_ws, window
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
import json
import redis
from datetime import datetime
from analytics.constants.redis_keys import RedisKeys
from analytics.core.spark_session import create_spark_session
from analytics.dq import calculate_quality_score, check_validity, detect_anomalies
from analytics.dq.completeness import check_completeness
from analytics.schema.patient_schema import PATIENT_SCHEMA
from config import config


class DataQualityStreamingEngine:
    def __init__(self):

        print("[1/7] Creating Spark Session...")
        # self.spark = SparkSession.builder \
        #     .appName("DataGuardian-Streaming") \
        #     .master(config.SPARK_MASTER) \
        #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        #     .config("spark.sql.streaming.checkpointLocation", config.CHECKPOINT_DIR) \
        #     .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        #     .config("spark.executor.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        #     .getOrCreate()
        self.spark = create_spark_session()

        self.spark.sparkContext.setLogLevel("WARN")
        print("    Spark session created\n")
        print("[2/7] Connecting to Redis...")
        try:
            self.redis_client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                decode_responses=True
            )
            self.redis_client.ping()
            print("    Redis connected\n")
        except Exception as e:
            print(
                f"   ⚠ Redis unavailable: {e} (will continue without caching)\n")
            self.redis_client = None

        print("[3/7] Defining data schema...")
        self.schema = PATIENT_SCHEMA
        print("    Schema defined\n")

        print("[4/7] Validating DQ rules...")
        self.rules = config.DQ_RULES
        print(f"    Loaded {len(self.rules)} rule categories\n")

    def create_stream_input(self):
        """
        Read from Kafka topic "patient_data"
        Returns: Streaming DataFrame
        """

        print("[5/7] Connecting to Kafka...")
        print(f"   Broker: {config.KAFKA_BROKERS}")
        print(f"   Topic: {config.KAFKA_RAW_TOPIC}")

        df_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BROKERS) \
            .option("subscribe", config.KAFKA_RAW_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("    Kafka connection established\n")

        return df_stream

    def parse_json(self, df_raw):

        df_parsed = df_raw.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*") \
            .withColumn("ingestion_time", current_timestamp())

        return df_parsed

    # def check_completeness(self, df):

    #     required_fields = self.rules["completeness"]["required_fields"]
    #     for field in required_fields:
    #         df = df.withColumn(
    #             f"missing_{field}",
    #             col(field).isNull()
    #         )
    #     df = df.withColumn(
    #         "missing_fields",
    #         concat_ws(",",
    #                   *[when(col(f"missing_{f}"), lit(f))
    #                     for f in required_fields]
    #                   )
    #     )
    #     df = df.withColumn(
    #         "completeness_score",
    #         sum([
    #             when(~col(f"missing_{f}"), lit(1)).otherwise(lit(0))
    #             for f in required_fields
    #         ])
    #     )

    #     return df

    # def check_validity(self, df):
    #     validity_rules = self.rules["validity"]

    #     # Age validation
    #     if "age" in validity_rules:
    #         age_rule = validity_rules["age"]
    #         df = df.withColumn(
    #             "age_valid",
    #             when(col("age").isNull(), None)
    #             .when((col("age") >= age_rule["min"]) &
    #                   (col("age") <= age_rule["max"]), True)
    #             .otherwise(False)
    #         )

    #     # BP validation
    #     if "bp_systolic" in validity_rules:
    #         bp_rule = validity_rules["bp_systolic"]
    #         df = df.withColumn(
    #             "bp_valid",
    #             when(col("bp_systolic").isNull(), None)
    #             .when((col("bp_systolic") >= bp_rule["min"]) &
    #                   (col("bp_systolic") <= bp_rule["max"]), True)
    #             .otherwise(False)
    #         )

    #     # Temperature validation
    #     if "temperature" in validity_rules:
    #         temp_rule = validity_rules["temperature"]
    #         df = df.withColumn(
    #             "temp_valid",
    #             when(col("temperature").isNull(), None)
    #             .when((col("temperature") >= temp_rule["min"]) &
    #                   (col("temperature") <= temp_rule["max"]), True)
    #             .otherwise(False)
    #         )

    #     return df

    # def detect_anomalies(self, df):
    #     z_threshold = self.rules["anomaly_detection"]["z_score_threshold"]
    #     df = df.withWatermark("ingestion_time", "10 minutes")
    #     stats = df.groupBy(
    #         window(col("ingestion_time"), "1 minute"),
    #         "patient_id"
    #     ).agg(
    #         avg("age").alias("age_mean"),
    #         stddev("age").alias("age_stddev"),
    #         avg("bp_systolic").alias("bp_mean"),
    #         stddev("bp_systolic").alias("bp_stddev")
    #     )
    #     df = df.join(
    #         stats,
    #         (col("patient_id") == stats["patient_id"]) &
    #         (window(col("ingestion_time"), "1 minute") == stats["window"]),
    #         "inner"
    #     )

    #     df = df.withColumn(
    #         "age_zscore",
    #         when(col("age_stddev").isNotNull() & (col("age_stddev") > 0),
    #              abs((col("age") - col("age_mean")) / col("age_stddev"))
    #              ).otherwise(0)
    #     ).withColumn(
    #         "age_anomaly",
    #         when(col("age_zscore") > z_threshold, True).otherwise(False)
    #     ).withColumn(
    #         "bp_zscore",
    #         when(col("bp_stddev").isNotNull() & (col("bp_stddev") > 0),
    #              abs((col("bp_systolic") - col("bp_mean")) / col("bp_stddev"))
    #              ).otherwise(0)
    #     ).withColumn(
    #         "bp_anomaly",
    #         when(col("bp_zscore") > z_threshold, True).otherwise(False)
    #     )

    #     return df

    # # def detect_anomalies(self, df):
    # #     z_threshold = self.rules["anomaly_detection"]["z_score_threshold"]
    # #     window_size = self.rules["anomaly_detection"]["window_size"]

    # #
    # #     df = df.withWatermark("ingestion_time", "10 minutes")

    # #     stats = df.groupBy(
    # #         window("ingestion_time", "1 minute"),
    # #         "patient_id"
    # #     ).agg(
    # #         avg("age").alias("age_mean"),
    # #         stddev("age").alias("age_stddev")
    # #     )
    # #     df = df.join(stats, ["patient_id"])
    # #     window_spec = Window.partitionBy("patient_id") \
    # #         .orderBy(col("ingestion_time").desc()) \
    # #         .rowsBetween(0, window_size - 1)
    # #     df = df.withColumn("age_mean", avg("age").over(window_spec)) \
    # #         .withColumn(
    # #         "age_stddev", stddev("age").over(window_spec)
    # #     ) \
    # #         .withColumn(
    # #         "age_zscore",
    # #         when(col("age_stddev").isNotNull() & (col("age_stddev") > 0),
    # #              abs((col("age") - col("age_mean")) / col("age_stddev"))
    # #              ).otherwise(0)
    # #     ) \
    # #         .withColumn(
    # #         "age_anomaly",
    # #         when(col("age_zscore") > z_threshold, True).otherwise(False)
    # #     )

    # #     df = df.withColumn(
    # #         "bp_mean", avg("bp_systolic").over(window_spec)
    # #     ) \
    # #         .withColumn(
    # #         "bp_stddev", stddev("bp_systolic").over(window_spec)
    # #     ) \
    # #         .withColumn(
    # #         "bp_zscore",
    # #         when(col("bp_stddev").isNotNull() & (col("bp_stddev") > 0),
    # #              abs((col("bp_systolic") - col("bp_mean")) / col("bp_stddev"))
    # #              ).otherwise(0)
    # #     ) \
    # #         .withColumn(
    # #         "bp_anomaly",
    # #         when(col("bp_zscore") > z_threshold, True).otherwise(False)
    # #     )

    # #     return df

    # def calculate_quality_score(self, df):
    #     """
    #     100 = Perfect data
    #     75 = Minor issues (1 validation failed)
    #     50 = Major issues (multiple validations failed)
    #     25 = Incomplete data
    #     0 = Critical issues
    #     """

    #     df = df.withColumn(
    #         "has_issues",
    #         (col("completeness_score") < 4) |
    #         (col("age_valid") == False) |
    #         (col("bp_valid") == False) |
    #         (col("temp_valid") == False) |
    #         (col("age_anomaly") == True) |
    #         (col("bp_anomaly") == True)
    #     )

    #     df = df.withColumn(
    #         "quality_score",
    #         when(col("completeness_score") < 4, 25)  # Incomplete
    #         .when(col("has_issues") == False, 100)   # Perfect
    #         .when(col("age_anomaly") | col("bp_anomaly"), 50)  # Anomaly
    #         .when((col("age_valid") == False) | (col("bp_valid") == False), 75)
    #         .otherwise(100)
    #     )

    #     return df

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
            .option("checkpointLocation", f"{config.CHECKPOINT_DIR}/console") \
            .start()

        return query

    def output_to_kafka(self, df_issues):
        df_kafka_format = df_issues.select(
            to_json(struct("*")).alias("value")
        )

        query = df_kafka_format.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BROKERS) \
            .option("topic", config.KAFKA_ISSUES_TOPIC) \
            .option("checkpointLocation", f"{config.CHECKPOINT_DIR}/kafka") \
            .start()

        return query

    def output_to_parquet(self, df):
        query = df.writeStream \
            .format("parquet") \
            .trigger(processingTime="1 minute")\
            .option(
                "path",
                config.DATA_LAKE_PATH
            ) \
            .option(
                "checkpointLocation",
                f"{config.CHECKPOINT_DIR}/parquet"
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
                    f"{config.CHECKPOINT_DIR}/redis"
                ) \
                .start()

            print("[7/7] Streaming pipeline active!\n")
            print("=" * 80)
            print(" Listening for incoming data...")
            print(f"  Input: Kafka topic '{config.KAFKA_RAW_TOPIC}'")
            print(
                f"  Output: Console + Kafka topic '{config.KAFKA_ISSUES_TOPIC}'")
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
