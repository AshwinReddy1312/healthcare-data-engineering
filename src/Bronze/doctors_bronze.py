from pyspark.sql import SparkSession
from src.common.spark_session import create_spark_session
from src.common.reader import read_csv

spark = create_spark_session("Health Care Bronze Pipeline")
doctors_df  = read_csv(spark,"data/raw/doctors.csv")
# Display Schema
doctors_df.printSchema()
# Show Data
doctors_df.show(10)
# Number of records
print(f"Total Records: {doctors_df.count()}")

from pyspark.sql.functions import col,count,when

doctors_df.select([
    count(when(col(column).isNull(), column)).alias(column)
    for column in doctors_df.columns
]).show()

print("Duplicate Doctor IDs")

doctors_df.groupBy("doctor_id") \
    .count() \
    .filter("count > 1") \
    .show()
    
doctors_df.printSchema()