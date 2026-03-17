from pyspark.sql import SparkSession
from util import *

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Assignment2") \
    .getOrCreate()

df = create_creditcard_df(spark)

original_partitions = df.rdd.getNumPartitions()
print("Original Partitions:", original_partitions)

df_repart = df.repartition(5)
print("After Repartition:", df_repart.rdd.getNumPartitions())

df_coalesce = df_repart.coalesce(original_partitions)
print("After Coalesce:", df_coalesce.rdd.getNumPartitions())

final_df = get_masked_df(df)
final_df.show(truncate=False)
spark.stop()