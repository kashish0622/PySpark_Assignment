
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
def create_spark():
    """
    Fully Windows-safe SparkSession (fixes NativeIO errors).
    """
    return (
    SparkSession.builder.appName("Assignment5").master("local[*]")
    .config("spark.sql.warehouse.dir", "C:/spark_warehouse")
    .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.Hdfs")
        .enableHiveSupport()
        .getOrCreate()

    )


def dynamic_schema(schema_dict):
    """
    Creates StructType dynamically from dictionary.
    """
    fields = []
    for col_name, dtype in schema_dict.items():
        if dtype == "int":
            fields.append(StructField(col_name, IntegerType(), True))
        else:
            fields.append(StructField(col_name, StringType(), True))
    return StructType(fields)


def create_employee_df(spark):
    schema_dict = {
        "employee_id": "int",
        "employee_name": "str",
        "department": "str",
        "State": "str",
        "salary": "int",
        "Age": "int"
    }

    schema = dynamic_schema(schema_dict)

    data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    return spark.createDataFrame(data, schema)


def create_department_df(spark):
    schema = dynamic_schema({
        "dept_id": "str",
        "dept_name": "str"
    })

    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    return spark.createDataFrame(data, schema)


def create_country_df(spark):
    schema = dynamic_schema({
        "country_code": "str",
        "country_name": "str"
    })

    data = [
        ("ny", "newyork"),
        ("ca", "california"),
        ("uk", "russia")
    ]

    return spark.createDataFrame(data, schema)