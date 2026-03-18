import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from util import (
    read_json,
    flatten_df,
    get_count,
    filter_data,
    to_snake_case,
    add_date_columns
)


def get_spark():
    return (
        SparkSession.builder
        .appName("Test_JSON_Flattener")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.Hdfs")
        .getOrCreate()
    )


def test_read_json(tmp_path):
    spark = get_spark()

    # ✅ Create sample JSON file
    json_path = tmp_path / "sample.json"
    json_path.write_text("""
    {
      "id": 1001,
      "properties": {"name": "StoreA", "storeSize": 400},
      "employees": [{"empId": 1, "empName": "Alan"}]
    }
    """)

    df = read_json(spark, str(json_path))
    assert df.count() == 1
    assert "properties" in df.columns
    assert "employees" in df.columns

    spark.stop()


def test_flatten_df():
    spark = get_spark()

    data = [{
        "id": 1001,
        "properties": {"name": "ABC", "storeSize": 300},
        "employees": [{"empId": 10, "empName": "John"}]
    }]

    df = spark.read.json(spark.sparkContext.parallelize([str(data)]))
    flat = flatten_df(df)

    assert "name" in flat.columns
    assert "storeSize" in flat.columns
    assert "empId" in flat.columns
    assert "empName" in flat.columns

    spark.stop()


def test_get_count():
    spark = get_spark()
    df = spark.createDataFrame([(1,), (2,), (3,)], ["num"])

    assert get_count(df) == 3
    spark.stop()


def test_filter_data():
    spark = get_spark()

    df = spark.createDataFrame(
        [(1001, "a"), (1002, "b")],
        ["id", "val"]
    )

    filtered = filter_data(df)
    assert filtered.count() == 1
    assert filtered.first()["id"] == 1001

    spark.stop()


def test_to_snake_case():
    spark = get_spark()

    df = spark.createDataFrame(
        [(1, "A", 100, 10, "John")],
        ["id", "name", "storeSize", "empId", "empName"]
    )

    df2 = to_snake_case(df)

    assert df2.columns == ["id", "name", "store_size", "emp_id", "emp_name"]

    spark.stop()


def test_add_date_columns():
    spark = get_spark()

    df = spark.createDataFrame(
        [(1, "A", 100, 10, "John")],
        ["id", "name", "store_size", "emp_id", "emp_name"]
    )

    df2 = add_date_columns(df)

    assert "load_date" in df2.columns
    assert "year" in df2.columns
    assert "month" in df2.columns
    assert "day" in df2.columns

    spark.stop()