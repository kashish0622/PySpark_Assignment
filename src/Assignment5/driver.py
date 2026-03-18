# test_assignment5.py

from driver import assignment_operations
from util import create_employee_df, create_department_df, create_country_df, create_spark


def test_employee_df_creation():
    spark = create_spark()
    df = create_employee_df(spark)

    assert df.count() == 7
    assert set(df.columns) == {
        "employee_id", "employee_name", "department", "State", "salary", "Age"
    }

    spark.stop()


def test_department_df_creation():
    spark = create_spark()
    df = create_department_df(spark)

    assert df.count() == 5
    assert "dept_id" in df.columns
    assert "dept_name" in df.columns

    spark.stop()


def test_country_df_creation():
    spark = create_spark()
    df = create_country_df(spark)

    assert df.count() == 3
    assert "country_code" in df.columns
    assert "country_name" in df.columns

    spark.stop()


def test_assignment_operations():
    """
    Full integration test for driver.py
    """

    final_df, avg_salary_df, employees_with_m = assignment_operations()

    # ✅ Test avg salary output
    assert avg_salary_df.count() == 3   # (D101, D102, D103)

    # ✅ employees starting with m
    assert employees_with_m.count() == 2  # michel, maria

    # ✅ check lowercase + load_date
    assert "load_date" in final_df.columns
    assert all(col.islower() for col in final_df.columns)

    # ✅ check country replaced
    assert "state" in final_df.columns
    mapped_states = [r["state"] for r in final_df.select("state").collect()]
    assert "newyork" in mapped_states or "california" in mapped_states