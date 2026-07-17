from src.common.spark_session import create_spark_session
from src.common.reader import read_csv
from pyspark.sql.functions import col, trim, initcap

# Create Spark Session
spark = create_spark_session("Healthcare Silver Pipeline")

# Read Data
prescriptions_df = read_csv(spark, "data/raw/prescriptions(1).csv")

# Original Record Count
print(f"Original Records: {prescriptions_df.count()}")

# ---------------------------------------------------
# Remove Duplicate Prescriptions
# ---------------------------------------------------
prescriptions_df = prescriptions_df.dropDuplicates(["prescription_id"])

# ---------------------------------------------------
# Remove Records with Null Prescription ID
# ---------------------------------------------------
prescriptions_df = prescriptions_df.filter(
    col("prescription_id").isNotNull()
)

# ---------------------------------------------------
# Remove Records with Null Appointment ID
# ---------------------------------------------------
prescriptions_df = prescriptions_df.filter(
    col("appointment_id").isNotNull()
)

# ---------------------------------------------------
# Remove Records with Null Medicine Name
# ---------------------------------------------------
prescriptions_df = prescriptions_df.filter(
    col("medicine").isNotNull()
)

# ---------------------------------------------------
# Remove Extra Spaces from Medicine Name
# ---------------------------------------------------
prescriptions_df = prescriptions_df.withColumn(
    "medicine",
    trim(col("medicine"))
)

# ---------------------------------------------------
# Standardize Medicine Name
# ---------------------------------------------------
prescriptions_df = prescriptions_df.withColumn(
    "medicine",
    initcap(col("medicine"))
)

# ---------------------------------------------------
# Display Schema
# ---------------------------------------------------
prescriptions_df.printSchema()

# ---------------------------------------------------
# Display Data
# ---------------------------------------------------
prescriptions_df.show(10)

# ---------------------------------------------------
# Final Record Count
# ---------------------------------------------------
print(f"Silver Records: {prescriptions_df.count()}")

# Stop Spark Session
spark.stop()