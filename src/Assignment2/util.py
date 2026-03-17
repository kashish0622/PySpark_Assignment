from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def create_creditcard_df(spark):
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    df = spark.createDataFrame(data, ["card_number"])
    return df

def mask_card(card_number):
    return "*" * 12 + card_number[-4:]

mask_udf = udf(mask_card, StringType())

def get_masked_df(df):
    return df.withColumn("masked_card_number", mask_udf(df["card_number"]))