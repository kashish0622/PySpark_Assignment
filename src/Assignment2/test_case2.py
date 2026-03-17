from util import *

def test_create_df(spark):
    df = create_creditcard_df(spark)
    assert df.count() == 5


def test_mask_function(spark):
    df = create_creditcard_df(spark)
    masked_df = get_masked_df(df)

    result = masked_df.collect()

    for row in result:
        assert len(row["masked_card_number"]) == 16
        assert row["masked_card_number"].startswith("************")


def test_partitions(spark):
    df = create_creditcard_df(spark)

    # original
    original = df.rdd.getNumPartitions()

    df_repart = df.repartition(5)
    assert df_repart.rdd.getNumPartitions() == 5

    df_coalesce = df_repart.coalesce(original)
    assert df_coalesce.rdd.getNumPartitions() == original