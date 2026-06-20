from pyspark.sql.functions import col, when


def calculate_quality_score(df):
    """
    100 = Perfect data
    75 = Minor issues (1 validation failed)
    50 = Major issues (multiple validations failed)
    25 = Incomplete data
    0 = Critical issues
    """

    df = df.withColumn(
        "has_issues",
        (col("completeness_score") < 4) |
        (col("age_valid") == False) |
        (col("bp_valid") == False) |
        (col("temp_valid") == False) |
        (col("age_anomaly") == True) |
        (col("bp_anomaly") == True)
    )

    df = df.withColumn(
        "quality_score",
        when(col("completeness_score") < 4, 25)  # Incomplete
        .when(col("has_issues") == False, 100)   # Perfect
        .when(col("age_anomaly") | col("bp_anomaly"), 50)  # Anomaly
        .when((col("age_valid") == False) | (col("bp_valid") == False), 75)
        .when(
            (col("age_valid") == False) |
            (col("bp_valid") == False) |
            (col("temp_valid") == False),
            75
        )
        .otherwise(100)
    )

    return df
