
import os
import shutil
from util import (
    get_spark,
    get_df,
    rename_cols,
    last_7_days,
    add_login_date,
    write_csv,
)
from pyspark.sql.functions import col


def test_get_df():
    spark = get_spark("test_get_df")
    df = get_df(spark)
    assert df.count() == 8
    assert len(df.columns) == 4
    spark.stop()


def test_rename_cols():
    spark = get_spark("test_rename_cols")
    df = rename_cols(get_df(spark))
    assert df.columns == ["log_id", "user_id", "user_activity", "time_stamp"]
    spark.stop()

def test_last_7_days():
    spark = get_spark("test_last_7_days")
    df = rename_cols(get_df(spark))

    result = last_7_days(df)
    assert "user_id" in result.columns
    assert "actions" in result.columns
    assert result.count() >= 1
    spark.stop()

def test_add_login_date():
    spark = get_spark("test_add_login_date")
    df = rename_cols(get_df(spark))
    df2 = add_login_date(df)
    first_row = df2.select("login_date").first()[0]
    assert first_row is not None
    spark.stop()

def test_write_csv(tmp_path):
    spark = get_spark("test_write_csv")
    df = rename_cols(get_df(spark))
    output_path = str(tmp_path / "csv_output")
    write_csv(df, output_path)
    assert os.path.exists(output_path)
    spark.stop()


def test_write_table():

    spark = get_spark("test_write_table")
    spark.sql("CREATE DATABASE IF NOT EXISTS user_db")
    df = rename_cols(get_df(spark))

    try:
        df.write.mode("overwrite").saveAsTable("user_db.login_details")
        success = True
    except Exception:
        success = False

    assert success is True

    spark.stop()