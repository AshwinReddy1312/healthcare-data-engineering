from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Healthcare Bronze Pipeline").getOrCreate()
patients_df  = spark.read.csv("data/raw/patients.csv",header=True,inferSchema=True)
# Display Schema
patients_df.printSchema()
# Show Data
patients_df.show(10)
# Number of records
print(f"Total Records: {patients_df.count()}")