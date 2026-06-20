from pyspark.sql.functions import col, when


def check_validity(df, rules):
    validity_rules = rules["validity"]

    # Age validation
    if "age" in validity_rules:
        age_rule = validity_rules["age"]
        df = df.withColumn(
            "age_valid",
            when(col("age").isNull(), None)
            .when((col("age") >= age_rule["min"]) &
                  (col("age") <= age_rule["max"]), True)
            .otherwise(False)
        )

    # BP validation
    if "bp_systolic" in validity_rules:
        bp_rule = validity_rules["bp_systolic"]
        df = df.withColumn(
            "bp_valid",
            when(col("bp_systolic").isNull(), None)
            .when((col("bp_systolic") >= bp_rule["min"]) &
                  (col("bp_systolic") <= bp_rule["max"]), True)
            .otherwise(False)
        )

    # Temperature validation
    if "temperature" in validity_rules:
        temp_rule = validity_rules["temperature"]
        df = df.withColumn(
            "temp_valid",
            when(col("temperature").isNull(), None)
            .when((col("temperature") >= temp_rule["min"]) &
                  (col("temperature") <= temp_rule["max"]), True)
            .otherwise(False)
        )

    return df
