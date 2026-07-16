from src.common.spark_session import create_spark_session
from src.common.reader import read_csv
from pyspark.sql.functions import col, initcap


spark = create_spark_session("Healthcare Silver Pipeline")
patients_df = read_csv(spark, "data/raw/patients.csv")

# Display Original Record Count
print(f"Original Records: {patients_df.count()}")

# ---------------------------------------------------
# 1. Remove Duplicate Patients
# ---------------------------------------------------
patients_df = patients_df.dropDuplicates(["patient_id"])

# ---------------------------------------------------
# 2. Remove Records with Null Patient Name
# ---------------------------------------------------
patients_df = patients_df.filter(col("patient_name").isNotNull())

# ---------------------------------------------------
# 3. Replace Null City with 'Unknown'
# ---------------------------------------------------
patients_df = patients_df.fillna({"city": "Unknown"})

# ---------------------------------------------------
# 4. Standardize Gender Values
# male, MALE -> Male
# female, FEMALE -> Female
# ---------------------------------------------------
patients_df = patients_df.withColumn(
    "gender",
    initcap(col("gender"))
)

# ---------------------------------------------------
# 5. Validate Age
# Keep only ages between 0 and 120
# ---------------------------------------------------
patients_df = patients_df.filter(
    (col("age") >= 0) &
    (col("age") <= 120)
)

# ---------------------------------------------------
# Show Cleaned Data
# ---------------------------------------------------
patients_df.printSchema()

patients_df.show(10)

print(f"Silver Records: {patients_df.count()}")

spark.stop()