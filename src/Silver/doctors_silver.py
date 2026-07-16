from src.common.spark_session import create_spark_session
from src.common.reader import read_csv
from spark.sql.functions import col, initcap, trim

# Create Spark Session
spark = create_spark_session("Healthcare Silver Pipeline")

# Read Bronze Data
doctors_df = read_csv(spark, "data/raw/doctors.csv")

# Original Record Count
print(f"Original Records: {doctors_df.count()}")

# ---------------------------------------------------
# 1. Remove Duplicate Doctors
# ---------------------------------------------------
doctors_df = doctors_df.dropDuplicates(["doctor_id"])

# ---------------------------------------------------
# 2. Remove Records with Null Doctor Name
# ---------------------------------------------------
doctors_df = doctors_df.filter(col("doctor_name").isNotNull())

# ---------------------------------------------------
# 3. Remove Extra Spaces from Doctor Name
# ---------------------------------------------------
doctors_df = doctors_df.withColumn(
    "doctor_name",
    trim(col("doctor_name"))
)

# ---------------------------------------------------
# 4. Standardize Doctor Name
# ---------------------------------------------------
doctors_df = doctors_df.withColumn(
    "doctor_name",
    initcap(col("doctor_name"))
)

# ---------------------------------------------------
# 5. Remove Records with Null Specialization
# ---------------------------------------------------
doctors_df = doctors_df.filter(
    col("specialization").isNotNull()
)

# ---------------------------------------------------
# Show Cleaned Data
# ---------------------------------------------------
doctors_df.printSchema()

doctors_df.show(10)

print(f"Silver Records: {doctors_df.count()}")

spark.stop()