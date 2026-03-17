from pyspark.sql.functions import (
    col,
    collect_set,
    array_contains,
    size,
    countDistinct
)

from util import purchase_data_df, product_data_df
def test_only_iphone13(spark):
    purchase_df = purchase_data_df(spark)

    result_df = purchase_df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products")) \
        .filter(
            (size(col("products")) == 1) &
            (array_contains(col("products"), "iphone13"))
        ) \
        .select("customer")

    result = [row.customer for row in result_df.collect()]

    assert result == [4]

def test_upgraded_customers(spark):
    purchase_df = purchase_data_df(spark)

    iphone13 = purchase_df.filter(col("product_model") == "iphone13").select("customer")
    iphone14 = purchase_df.filter(col("product_model") == "iphone14").select("customer")

    upgraded = iphone13.intersect(iphone14)

    result = sorted([row.customer for row in upgraded.collect()])

    assert result == [1, 3]

def test_all_products(spark):
    purchase_df = purchase_data_df(spark)
    product_df = product_data_df(spark)

    total_products = product_df.select("product_model").distinct().count()

    result_df = purchase_df.groupBy("customer") \
        .agg(countDistinct("product_model").alias("cnt")) \
        .filter(col("cnt") == total_products) \
        .select("customer")

    result = [row.customer for row in result_df.collect()]

    assert result == [1]
