from pyspark.sql import SparkSession
from src.common.spark_session import create_spark_session
from src.common.reader import read_csv

spark = create_spark_session("Healthcare Bronze Pipeline")
appointments_df  = read_csv(spark,"data/raw/appointments(1).csv")
# Display Schema
appointments_df.printSchema()
# Show Data
appointments_df.show(10)
# Number of records
print(f"Total Records: {appointments_df.count()}")


from pyspark.sql.functions import col, count, when
print("Null Values in Each Column")

appointments_df.select([
    count(when(col(column).isNull(), column)).alias(column)
    for column in appointments_df.columns
]).show()

print("Duplicate Appointment IDs")

appointments_df.groupBy("appointment_id") \
    .count() \
    .filter("count > 1") \
    .show()
    
spark.stop()