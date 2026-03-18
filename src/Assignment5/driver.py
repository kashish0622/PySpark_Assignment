# driver.py
from util import *
from pyspark.sql.functions import *


def assignment_operations():

    spark = create_spark()

    # Load all 3 dataframes
    employee_df = create_employee_df(spark)
    department_df = create_department_df(spark)
    country_df = create_country_df(spark)

    print("Employee DF:")
    employee_df.show()

    # 1. AVG salary per department
    avg_salary_df = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    print("AVG Salary DF:")
    avg_salary_df.show()

    # 2. Name starts with 'm'
    employees_m = employee_df.filter(col("employee_name").startswith("m"))
    employees_with_dept = employees_m.join(department_df, employee_df.department == department_df.dept_id, "left")
    print("Employees starting with M:")
    employees_with_dept.show()

    # 3. Add bonus column
    employee_bonus_df = employee_df.withColumn("bonus", col("salary") * 2)

    # 4. Reorder columns
    employee_reordered = employee_bonus_df.select(
        "employee_id", "employee_name", "salary", "State", "Age", "department", "bonus"
    )

    print("Reordered DF:")
    employee_reordered.show()

    # 5. Dynamic joins
    inner_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
    left_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
    right_join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")

    # 6. Replace State with country_name
    employee_country_df = employee_df.join(
        country_df,
        employee_df.State == country_df.country_code,
        "left"
    ).drop("State").withColumnRenamed("country_name", "State")

    print("Country Mapped DF:")
    employee_country_df.show()

    # 7. Columns lowercase + load_date column
    final_df = employee_country_df
    for c in final_df.columns:
        final_df = final_df.withColumnRenamed(c, c.lower())

    final_df = final_df.withColumn("load_date", current_date())

    final_df.show()

    # 8. Write two EXTERNAL TABLES (CSV & Parquet)
    spark.sql("CREATE DATABASE IF NOT EXISTS emp_db")

    final_df.write \
        .mode("overwrite") \
        .option("path", "emp_csv_output") \
        .format("csv") \
        .saveAsTable("emp_db.employee_csv")

    final_df.write \
        .mode("overwrite") \
        .option("path", "emp_parquet_output") \
        .format("parquet") \
        .saveAsTable("emp_db.employee_parquet")

    print("External tables created successfully!")

    return final_df, avg_salary_df, employees_with_dept


if __name__ == "__main__":
    assignment_operations()
