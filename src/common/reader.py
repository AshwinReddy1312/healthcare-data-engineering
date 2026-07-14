def read_csv(spark, file_path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )