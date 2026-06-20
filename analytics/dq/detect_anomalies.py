from pyspark.sql.functions import col, when, avg, stddev, window, abs


def detect_anomalies(df, rules):
    z_threshold = rules["anomaly_detection"]["z_score_threshold"]
    df = df.withWatermark("ingestion_time", "10 minutes")
    stats = df.groupBy(
        window(col("ingestion_time"), "5 minutes"),
        "hospital"
    ).agg(
        avg("age").alias("age_mean"),
        stddev("age").alias("age_stddev"),
        avg("bp_systolic").alias("bp_mean"),
        stddev("bp_systolic").alias("bp_stddev")
    )
    df = df.join(
        stats,
        (col("patient_id") == stats["patient_id"]) &
        (window(col("ingestion_time"), "1 minute") == stats["window"]),
        "inner"
    )

    df = df.withColumn(
        "age_zscore",
        when(col("age_stddev").isNotNull() & (col("age_stddev") > 0),
             abs((col("age") - col("age_mean")) / col("age_stddev"))
             ).otherwise(0)
    ).withColumn(
        "age_anomaly",
        when(col("age_zscore") > z_threshold, True).otherwise(False)
    ).withColumn(
        "bp_zscore",
        when(col("bp_stddev").isNotNull() & (col("bp_stddev") > 0),
             abs((col("bp_systolic") - col("bp_mean")) / col("bp_stddev"))
             ).otherwise(0)
    ).withColumn(
        "bp_anomaly",
        when(col("bp_zscore") > z_threshold, True).otherwise(False)
    )

    return df
