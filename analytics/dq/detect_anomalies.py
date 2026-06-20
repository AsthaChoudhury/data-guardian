from pyspark.sql.functions import col, when, avg, stddev, abs, window, expr


def detect_anomalies(df, rules):

    z_threshold = rules["anomaly_detection"]["z_score_threshold"]
    window_duration = "5 minutes"

    events = df.withWatermark("ingestion_time", "10 minutes")

    stats = (
        events
        .groupBy(
            window(col("ingestion_time"), window_duration),
            col("hospital")
        )
        .agg(
            avg("age").alias("age_mean"),
            stddev("age").alias("age_stddev"),
            avg("bp_systolic").alias("bp_mean"),
            stddev("bp_systolic").alias("bp_stddev")
        )
        .withColumn("window_end", col("window.end"))
        .withWatermark("window_end", "10 minutes")
    )
    lower_bound = col("s.window_end") - expr(f"INTERVAL {window_duration}")

    joined = (
        events.alias("e")
        .join(
            stats.alias("s"),
            (col("e.hospital") == col("s.hospital")) &
            (col("e.ingestion_time") >= lower_bound) &
            (col("e.ingestion_time") < col("s.window_end")),
            "left"
        )
    )

    df = joined.select(
        col("e.*"),
        col("s.age_mean"),
        col("s.age_stddev"),
        col("s.bp_mean"),
        col("s.bp_stddev")
    )

    df = df.withColumn(
        "age_zscore",
        when(
            (col("age_stddev").isNotNull()) & (col("age_stddev") > 0),
            abs((col("age") - col("age_mean")) / col("age_stddev"))
        ).otherwise(0)
    ).withColumn(
        "age_anomaly", col("age_zscore") > z_threshold
    ).withColumn(
        "bp_zscore",
        when(
            (col("bp_stddev").isNotNull()) & (col("bp_stddev") > 0),
            abs((col("bp_systolic") - col("bp_mean")) / col("bp_stddev"))
        ).otherwise(0)
    ).withColumn(
        "bp_anomaly", col("bp_zscore") > z_threshold
    )

    return df
