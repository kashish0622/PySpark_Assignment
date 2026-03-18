# test_assignment5.py

from util import (
    create_spark,
    dynamic_schema,
    create_employee_df,
    create_department_df,
    create_country_df,
)


def test_dynamic_schema():
    schema_dict = {"id": "int", "name": "str"}
    schema = dynamic_schema(schema_dict)

    assert len(schema.fields) == 2
    assert schema.fields[0].name == "id"
    assert schema.fields[1].name == "name"


def test_employee_df():
    spark = create_spark()
    df = create_employee_df(spark)

    # ✅ Row count should be EXACTLY 7
    assert df.count() == 7

    # ✅ Columns must match your util.py
    assert df.columns == [
        "employee_id", "employee_name", "department",
        "State", "salary", "Age"
    ]

    spark.stop()


def test_department_df():
    spark = create_spark()
    df = create_department_df(spark)

    assert df.count() == 5
    assert df.columns == ["dept_id", "dept_name"]

    spark.stop()


def test_country_df():
    spark = create_spark()
    df = create_country_df(spark)

    assert df.count() == 3
    assert df.columns == ["country_code", "country_name"]

    spark.stop()