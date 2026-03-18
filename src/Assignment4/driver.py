from pyspark.sql import SparkSession
from util import *

spark = SparkSession.builder \
    .appName("Assignment4_Java17") \
    .master("local[*]") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.Hdfs") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED") \
    .getOrCreate()

path = "nested_json_file.json"
df = read_json(spark, path)

df_flat = flatten_df(df)
df_filtered = filter_data(df_flat)
df_snake = to_snake_case(df_filtered)
df_final = add_date_columns(df_snake)


df_final.coalesce(1).write \
    .format("json") \
    .mode("overwrite") \
    .save("C:/spark_output/output")
