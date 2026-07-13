from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Healthcare Bronze Pipeline").getOrCreate()
doctors_df  = spark.read.csv("data/raw/doctors.csv",header=True,inferSchema=True)
# Display Schema
doctors_df.printSchema()
# Show Data
doctors_df.show(10)
# Number of records
print(f"Total Records: {doctors_df.count()}")