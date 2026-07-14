from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Healthcare Bronze Pipeline").getOrCreate()
doctors_df  = spark.read.csv("data/raw/doctors.csv",header=True,inferSchema=True)
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