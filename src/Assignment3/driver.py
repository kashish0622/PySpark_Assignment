from util import *
def main():
    spark = get_spark("1st")
    df = get_df(spark)
    df.show()

    spark = get_spark("2nd")
    df = rename_cols(get_df(spark))
    df.show()

    spark = get_spark("3rd")
    df = rename_cols(get_df(spark))
    last_7_days(df).show()

    spark = get_spark("4th")
    df = rename_cols(get_df(spark))
    add_login_date(df).show()

    spark = get_spark("5th")
    df = rename_cols(get_df(spark))
    write_csv(df, "output/csv_data")

    spark = get_spark("6th")
    spark.sql("CREATE DATABASE IF NOT EXISTS 'user'")
    df = rename_cols(get_df(spark))
    write_table(df)

if __name__ == "__main__":
    main()