from pyspark.sql.functions import *

# Read JSON
def read_json(spark, path):
    return spark.read.option("multiline", "true").json(path)


# Flatten DataFrame
def flatten_df(df):
    return df \
        .withColumn("name", col("properties.name")) \
        .withColumn("storeSize", col("properties.storeSize")) \
        .withColumn("employee", explode(col("employees"))) \
        .withColumn("empId", col("employee.empId")) \
        .withColumn("empName", col("employee.empName")) \
        .drop("properties", "employees", "employee")


# Count
def get_count(df):
    return df.count()


# Filter id = 1001
def filter_data(df):
    return df.filter(col("id") == 1001)


# Convert to snake_case (manual simple way)
def to_snake_case(df):
    return df.toDF(
        "id",
        "name",
        "store_size",   # manually converted
        "emp_id",
        "emp_name"
    )


# Add load_date and year/month/day
def add_date_columns(df):
    df = df.withColumn("load_date", current_date())

    df = df.withColumn("year", year("load_date")) \
           .withColumn("month", month("load_date")) \
           .withColumn("day", dayofmonth("load_date"))

    return df