from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, max, min, stddev,
    when, to_date, lag
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType, StructType, StructField, StringType, IntegerType, DoubleType
import json
import redis
from datetime import datetime, timedelta
from config import config


class BatchAnalyticsEngine:
    def __init__(self):
        print("\n" + "=" * 80)
        print("DataGuardian: Batch Analytics Engine")
        print("=" * 80 + "\n")

        # Spark session
        self.spark = SparkSession.builder \
            .appName("DataGuardian-BatchAnalytics") \
            .master(config.SPARK_MASTER) \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Redis
        try:
            self.redis_client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                decode_responses=True
            )
            self.redis_client.ping()
        except:
            self.redis_client = None

    def load_raw_assessments(self, run_date):
        print(f"[1/5] Loading raw assessments for {run_date}...")
        sample_data = [
            ("Hospital_A", "PAT_001", 45, 130, 36.5, 100, True, run_date),
            ("Hospital_A", "PAT_002", -5, 120, 36.8, 50, False, run_date),
            ("Hospital_B", "PAT_003", 62, 150, 37.0, 95, True, run_date),
            ("Hospital_B", "PAT_004", 58, 300, 36.9, 50, False, run_date),
            ("Hospital_C", "PAT_005", 71, 140, 45.5, 50, False, run_date),
        ]

        schema = StructType([
            StructField("hospital", StringType()),
            StructField("patient_id", StringType()),
            StructField("age", IntegerType()),
            StructField("bp_systolic", IntegerType()),
            StructField("temperature", DoubleType()),
            StructField("quality_score", IntegerType()),
            StructField("has_issues", BooleanType()),
            StructField("date", StringType())
        ])

        df = self.spark.createDataFrame(sample_data, schema)

        print(f"    Loaded {df.count()} records\n")

        return df

    def calculate_daily_metrics(self, df):

        print("[2/5] Calculating daily metrics...")

        daily_metrics = df.groupBy("hospital", "date").agg(
            count("*").alias("total_records"),
            sum(when(col("has_issues") == 1, 1)).alias("records_with_issues"),
            avg("quality_score").alias("avg_quality_score"),
            max("quality_score").alias("max_quality_score"),
            min("quality_score").alias("min_quality_score"),
            stddev("quality_score").alias("quality_variance")
        ) \
            .withColumn(
            "issue_rate",
            col("records_with_issues") / col("total_records") * 100
        ) \
            .withColumn(
            "health_status",
            when(col("avg_quality_score") >= 95, "excellent")
            .when(col("avg_quality_score") >= 85, "good")
            .when(col("avg_quality_score") >= 70, "fair")
            .otherwise("poor")
        )

        print(
            f" Calculated metrics for {daily_metrics.count()} hospital-days\n")

        return daily_metrics

    def identify_trends(self, daily_metrics):
        print("[3/5] Identifying trends...")

        daily_metrics = daily_metrics.withColumn(
            "date",
            to_date(col("date"))
        )

        window_trend = Window.partitionBy("hospital") \
            .orderBy(col("date")) \
            .rowsBetween(-6, 0)
        window_spec_lag = Window.partitionBy("hospital").orderBy("date")

        trends = daily_metrics.withColumn(
            "ma7_quality_score",
            avg("avg_quality_score").over(window_trend)
        ) \
            .withColumn(
            "ma7_issue_rate",
            avg("issue_rate").over(window_trend)
        ) \
            .withColumn(
            "trend_direction",
            when(
                col("ma7_quality_score") > lag(
                    "ma7_quality_score", 1, 0).over(window_spec_lag),
                "improving"
            ).otherwise("degrading")
        ) \
            .withColumn(
            "quality_change",
            col("ma7_quality_score") -
                lag("ma7_quality_score", 1, 0).over(window_spec_lag)
        )

        print(f"    Trend analysis complete\n")
        return trends

    def find_problem_patterns(self, df):
        print("[4/5] Finding problem patterns...")
        hospital_comparison = df.groupBy("hospital").agg(
            count("*").alias("total_records"),
            sum(
                when(col("has_issues"), 1).otherwise(0)
            ).alias("total_issues"),
            avg("quality_score").alias("avg_quality_score")
        ) \
            .withColumn(
            "issue_rate",
            col("total_issues") / col("total_records") * 100
        ) \
            .orderBy(col("issue_rate").desc())
        problematic_patients = df.filter(col("has_issues")) \
            .groupBy("patient_id", "hospital").count() \
            .orderBy(col("count").desc()) \
            .limit(10)

        print(f"    Found patterns\n")

        return hospital_comparison, problematic_patients

    def predict_future_issues(self, trends):

        print("[5/5] Predicting future issues...")

        predictions = trends.withColumn(
            "predicted_issue_risk",
            when(
                (col("trend_direction") == "degrading") & (
                    col("quality_variance") > 10),
                0.8
            ).when(
                col("trend_direction") == "degrading",
                0.6
            ).when(
                (col("trend_direction") == "improving") & (
                    col("quality_variance") < 5),
                0.2
            ).otherwise(0.4)
        ) \
            .select(
            col("hospital"),
            col("date"),
            col("avg_quality_score"),
            col("predicted_issue_risk"),
            col("trend_direction")
        )

        print(f"    Predictions generated\n")

        return predictions

    def display_results(self, daily_metrics, trends, hospital_comp, predictions):

        print("\n" + "=" * 80)
        print("DAILY METRICS")
        print("=" * 80)
        daily_metrics.show()

        print("\n" + "=" * 80)
        print("TRENDS (7-Day Moving Average)")
        print("=" * 80)
        trends.show()

        print("\n" + "=" * 80)
        print("HOSPITAL COMPARISON")
        print("=" * 80)
        hospital_comp.show()

        print("\n" + "=" * 80)
        print("FUTURE PREDICTIONS")
        print("=" * 80)
        predictions.show()

    def cache_results(self, daily_metrics, predictions):

        if not self.redis_client:
            return

        try:
            # Cache daily metrics
            latest_metrics = daily_metrics.limit(10).collect()

            for row in latest_metrics:
                hospital = row['hospital']
                data = {
                    "hospital": hospital,
                    "avgQualityScore": float(row['avg_quality_score']),
                    "issueRate": float(row['issue_rate']),
                    "totalRecords": int(row['total_records']),
                    "healthStatus": row['health_status'],
                    "timestamp": datetime.now().isoformat()
                }

                self.redis_client.setex(
                    f"dg:daily_metrics:{hospital}",
                    86400,  # 24 hour TTL
                    json.dumps(data)
                )
        except Exception as e:
            print(f"⚠ Redis caching error: {e}")

    def run(self, run_date=None):

        if run_date is None:
            run_date = (datetime.now() - timedelta(days=1)
                        ).strftime("%Y-%m-%d")

        print(f"\nRunning batch analytics for: {run_date}\n")

        try:
            # Load
            df = self.load_raw_assessments(run_date)

            # Analyze
            daily_metrics = self.calculate_daily_metrics(df)
            trends = self.identify_trends(daily_metrics)
            hospital_comp, problem_patients = self.find_problem_patterns(df)
            predictions = self.predict_future_issues(trends)

            # Display
            self.display_results(daily_metrics, trends,
                                 hospital_comp, predictions)

            # Cache
            self.cache_results(daily_metrics, predictions)

            print("\n" + "=" * 80)
            print(" Batch analysis complete!")
            print("=" * 80 + "\n")

        except Exception as e:
            print(f"Error: {e}")
            raise
        finally:
            if self.redis_client:
                self.redis_client.close()
            self.spark.stop()


if __name__ == "__main__":
    analytics = BatchAnalyticsEngine()
    analytics.run()
