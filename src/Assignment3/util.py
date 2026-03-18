from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

def get_spark(name):
    return SparkSession.builder.appName(name).getOrCreate()
def get_df(spark):
    schema = StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]
    return spark.createDataFrame(data, schema)

def rename_cols(df):
    new_cols = ["log_id", "user_id", "user_activity", "time_stamp"]
    return df.toDF(*new_cols)

def last_7_days(df):
    df = df.withColumn("time_stamp", to_timestamp(col("time_stamp")))
    return df.filter(col("time_stamp") >= date_sub(current_date(), 7)) \
        .groupBy("user_id") \
        .agg(count("*").alias("actions"))

def add_login_date(df):
    return df.withColumn("login_date", to_date(col("time_stamp")))

def write_csv(df, path):
    df.write.mode("overwrite").option("header", True).option("delimiter", ",").csv(path)

def write_table(df):
    df.write.mode("overwrite").saveAsTable("user.login_details")