from pyspark.sql import SparkSession
from util import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession \
        .builder \
        .appName("PySparkAssignment") \
        .master("local[*]") \
        .getOrCreate()
    purchased = purchase_data_df(spark)
    purchased.show()

    products = product_data_df(spark)
    products.show()

    # customers who bought only iphone
    only_iphone13 = purchased.groupBy("customer")\
         .agg(collect_set("product_model").alias("iphone13_only"))\
        .filter(col("iphone13_only") == array(lit("iphone13")))
    print("customers who have iphone 13 only: ")
    only_iphone13.show()

    # upgraded to iphone14
    iphone13_customer = purchased.filter(col("product_model") == ("iphone13")).select("customer")
    iphone14_customer = purchased.filter(col("product_model") == ("iphone14")).select("customer")
    upgraded_to_iphone14 = iphone13_customer.intersect(iphone14_customer)
    print("customers who have upgraded to iphone 14 : ")
    upgraded_to_iphone14.show()

    # bought all products

    all_products =  products.select("product_model").distinct().count()
    customer_all_products = purchased.groupBy("customer")\
        .agg(count_distinct("product_model").alias("product_count"))\
    .filter(col("product_count") == all_products)
    print("customers who have bought all products : ")
    customer_all_products.show()
if __name__ == "__main__":
    main()


