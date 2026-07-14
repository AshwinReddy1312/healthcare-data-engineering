from pyspark.sql import SparkSession
from src.common.spark_session import create_spark_session
from src.common.reader import read_csv

spark = create_spark_session("Healthcare Bronze Pipeline")
patients_df  = read_csv(spark,"data/raw/patients.csv")
# Display Schema
patients_df.printSchema()
# Show Data
patients_df.show(10)
# Number of records
print(f"Total Records: {patients_df.count()}")


from pyspark.sql.functions import col, count, when

print("Null Values in Each Column")
patients_df.select([
    count(when(col(column).isNull(), column)).alias(column)
    for column in patients_df.columns
]).show()

patients_df.groupBy("patient_id").count().filter("count > 1").show()# Write Bronze Data
patients_df.write .mode("overwrite").parquet("data/bronze/patients")

print("Bronze data written successfully!")

# Stop Spark
spark.stop()