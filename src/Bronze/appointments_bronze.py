from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Healthcare Bronze Pipeline").getOrCreate()
appointments_df  = spark.read.csv("data/raw/appointments(1).csv",header=True,inferSchema=True)
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