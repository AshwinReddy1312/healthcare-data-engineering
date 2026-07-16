from src.common.spark_session import create_spark_session
from src.common.reader import read_csv
from pyspark.sql.functions import col, trim, initcap, to_date

# Create Spark Session
spark = create_spark_session("Healthcare Silver Pipeline")

# Read Data
appointments_df = read_csv(spark, "data/raw/appointments(1).csv")

# Original Record Count
print(f"Original Records: {appointments_df.count()}")

# ---------------------------------------------------
# Remove Duplicate Appointments
# ---------------------------------------------------
appointments_df = appointments_df.dropDuplicates(["appointment_id"])

# ---------------------------------------------------
# Remove Records with Null Appointment ID
# ---------------------------------------------------
appointments_df = appointments_df.filter(
    col("appointment_id").isNotNull()
)

# ---------------------------------------------------
# Remove Records with Null Patient ID
# ---------------------------------------------------
appointments_df = appointments_df.filter(
    col("patient_id").isNotNull()
)

# ---------------------------------------------------
# Remove Records with Null Doctor ID
# ---------------------------------------------------
appointments_df = appointments_df.filter(
    col("doctor_id").isNotNull()
)

# ---------------------------------------------------
# Convert Appointment Date to Date Format
# Example: 14-07-2026 → 2026-07-14
# Change the format if your CSV is different
# ---------------------------------------------------
appointments_df = appointments_df.withColumn(
    "appointment_date",
    to_date(col("appointment_date"), "dd-MM-yyyy")
)

# ---------------------------------------------------
# Remove Invalid Dates
# ---------------------------------------------------
appointments_df = appointments_df.filter(
    col("appointment_date").isNotNull()
)

# ---------------------------------------------------
# Trim Spaces (Example)
# Uncomment if you have appointment_type column
# ---------------------------------------------------
# appointments_df = appointments_df.withColumn(
#     "appointment_type",
#     trim(col("appointment_type"))
# )

# ---------------------------------------------------
# Standardize Text (Example)
# Uncomment if you have appointment_type column
# ---------------------------------------------------
# appointments_df = appointments_df.withColumn(
#     "appointment_type",
#     initcap(col("appointment_type"))
# )

# ---------------------------------------------------
# Display Data
# ---------------------------------------------------
appointments_df.printSchema()

appointments_df.show(10)

print(f"Silver Records: {appointments_df.count()}")

# Stop Spark
spark.stop()