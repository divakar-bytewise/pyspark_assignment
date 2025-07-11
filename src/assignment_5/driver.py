from pyspark.sql import SparkSession
from src.assignment_5.util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
def main():
    # Create Spark session
    spark = SparkSession.builder.appName("Assignment5").enableHiveSupport().getOrCreate()

    # 1. Create DataFrames
    employee_df = create_employee_df(spark)
    department_df = create_department_df(spark)
    country_df = create_country_df(spark)

    # 2. Average salary by department
    avg_salary_df = calculate_avg_salary(employee_df)
    print("Average Salary by Department:")
    avg_salary_df.show()

    # 3. Employees whose name starts with 'm'
    employees_m_df = find_employees_starting_with_m(employee_df, department_df)
    print("Employees starting with 'm':")
    employees_m_df.show()

    # 4. Add bonus column
    employee_with_bonus_df = add_bonus_column(employee_df)
    print("Employee DataFrame with Bonus Column:")
    employee_with_bonus_df.show()

    # 5. Reorder columns
    reordered_employee_df = reorder_employee_columns(employee_df)
    print("Reordered Columns:")
    reordered_employee_df.show()

    # 6. Inner, Left, Right Joins
    inner_join_df, left_join_df, right_join_df = perform_joins(employee_df, department_df)
    print("Inner Join:")
    inner_join_df.show()

    print("Left Join:")
    left_join_df.show()

    print("Right Join:")
    right_join_df.show()

    # 7. Replace state with country name
    employee_with_country_df = replace_state_with_country(employee_df, country_df)
    print("Employee with Country Name:")
    employee_with_country_df.show()

    # 8. Convert columns to lowercase and add load_date
    final_df = convert_columns_lowercase_and_add_date(employee_with_country_df)
    print("Final DataFrame:")
    final_df.show()

    # 9. Write as external tables in both Parquet and CSV
    write_external_tables(final_df)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
