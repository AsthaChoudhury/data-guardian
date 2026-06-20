from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
PATIENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), nullable=True),
    StructField("hospital", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("bp_systolic", IntegerType(), nullable=True),
    StructField("bp_diastolic", IntegerType(), nullable=True),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("heart_rate", IntegerType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True)
])
