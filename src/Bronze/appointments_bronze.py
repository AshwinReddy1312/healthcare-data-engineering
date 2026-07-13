from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Healthcare Bronze Pipeline").getOrCreate()
appointments_df  = spark.read.csv("data/raw/appointments.csv",header=True,inferSchema=True)
# Display Schema
appointments_df.printSchema()
# Show Data
appointments_df.show(10)
# Number of records
print(f"Total Records: {appointments_df.count()}")