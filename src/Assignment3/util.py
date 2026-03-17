from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_date, col, current_date, date_sub, count

# Step 1: Create DataFrame
def create_df(spark):
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

def rename_columns(df):
    return df.toDF("log_id", "user_id", "user_activity", "time_stamp")

def add_login_date(df):
    return df.withColumn("login_date", to_date(col("time_stamp")))

def get_user_actions(df):
    filtered_df = df.filter(
        col("login_date") >= date_sub(current_date(), 7)
    )

    return filtered_df.groupBy("user_id").agg(
        count("user_activity").alias("action_count")
    )