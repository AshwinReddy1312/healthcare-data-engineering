from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Healthcare Bronze Pipeline").getOrCreate()
prescriptions_df  = spark.read.csv("data/raw/prescriptions.csv",header=True,inferSchema=True)
# Display Schema
prescriptions_df.printSchema()
# Show Data
prescriptions_df.show(10)
# Number of records
print(f"Total Records: {prescriptions_df.count()}")