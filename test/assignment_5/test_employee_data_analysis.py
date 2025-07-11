import pytest
from pyspark.sql import SparkSession
from src.assignment_5.util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[*]").appName("Assignment5Test").getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="module")
def employee_df(spark):
    return create_employee_df(spark)

@pytest.fixture(scope="module")
def department_df(spark):
    return create_department_df(spark)

@pytest.fixture(scope="module")
def country_df(spark):
    return create_country_df(spark)

def test_create_employee_df(employee_df):
    assert employee_df.count() == 7
    assert "employee_name" in employee_df.columns

def test_calculate_avg_salary(employee_df):
    avg_df = calculate_avg_salary(employee_df)
    assert "avg_salary" in avg_df.columns

def test_find_employees_starting_with_m(employee_df, department_df):
    filtered_df = find_employees_starting_with_m(employee_df, department_df)
    names = [row.employee_name.lower() for row in filtered_df.collect()]
    for name in names:
        assert name.startswith("m")

def test_add_bonus_column(employee_df):
    df_with_bonus = add_bonus_column(employee_df)
    assert "bonus" in df_with_bonus.columns
    for row in df_with_bonus.collect():
        assert row.bonus == row.salary * 2

def test_reorder_employee_columns(employee_df):
    reordered_df = reorder_employee_columns(employee_df)
    assert reordered_df.columns == ['employee_id', 'employee_name', 'salary', 'state', 'age', 'department']

def test_perform_joins(employee_df, department_df):
    inner, left, right = perform_joins(employee_df, department_df)
    assert inner.count() <= left.count()  # left join has equal or more rows than inner
    assert right.count() >= inner.count()  # right join has equal or more rows than inner

def test_replace_state_with_country(employee_df, country_df):
    df_with_country = replace_state_with_country(employee_df, country_df)
    assert "state" in df_with_country.columns

def test_convert_columns_lowercase_and_add_date(employee_df, country_df):
    df_with_country = replace_state_with_country(employee_df, country_df)
    final_df = convert_columns_lowercase_and_add_date(df_with_country)
    for col_name in final_df.columns:
        assert col_name == col_name.lower()
    assert "load_date" in final_df.columns
