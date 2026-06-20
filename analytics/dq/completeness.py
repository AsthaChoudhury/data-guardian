from pyspark.sql.functions import col, when, lit, concat_ws


def check_completeness(df, rules):

    required_fields = rules["completeness"]["required_fields"]

    for field in required_fields:
        df = df.withColumn(
            f"missing_{field}",
            col(field).isNull()
        )

    df = df.withColumn(
        "missing_fields",
        concat_ws(
            ",",
            *[
                when(col(f"missing_{field}"), lit(field))
                for field in required_fields
            ]
        )
    )

    score_expr = None

    for field in required_fields:

        expr = when(
            ~col(f"missing_{field}"),
            lit(1)
        ).otherwise(lit(0))

        score_expr = expr if score_expr is None else score_expr + expr

    df = df.withColumn(
        "completeness_score",
        score_expr
    )

    return df
