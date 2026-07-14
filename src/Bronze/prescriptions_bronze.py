from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,when
from src.common.spark_session import create_spark_session

spark = create_spark_session("Healthcare Bronze Pipeline")
prescriptions_df  = spark.read.csv("data/raw/prescriptions(1).csv",header=True,inferSchema=True)
# Display Schema
prescriptions_df.printSchema()
# Show Data
prescriptions_df.show(10)
# Number of records
print(f"Total Records: {prescriptions_df.count()}")

# Check Null Values
print("Null Values in Each Column")

prescriptions_df.select([
    count(when(col(column).isNull(), column)).alias(column)
    for column in prescriptions_df.columns
]).show()

# Check Duplicate Prescription IDs
print("Duplicate Prescription IDs")

prescriptions_df.groupBy("prescription_id") \
    .count() \
    .filter("count > 1") \
    .show()

# Stop Spark Session
spark.stop()