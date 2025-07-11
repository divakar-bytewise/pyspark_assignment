from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_date
from pyspark.sql.types import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

def get_spark_session():
    return SparkSession.builder.appName("EmployeeAnalysis").enableHiveSupport().getOrCreate()

def create_employee_df(spark):
    data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]
    schema = StructType([
        StructField("employee_id", IntegerType()),
        StructField("employee_name", StringType()),
        StructField("department", StringType()),
        StructField("state", StringType()),
        StructField("salary", IntegerType()),
        StructField("age", IntegerType())
    ])
    return spark.createDataFrame(data, schema)

def create_department_df(spark):
    data = [("D101", "sales"), ("D102", "finance"), ("D103", "marketing"), ("D104", "hr"), ("D105", "support")]
    schema = StructType([StructField("dept_id", StringType()), StructField("dept_name", StringType())])
    return spark.createDataFrame(data, schema)

def create_country_df(spark):
    data = [("ny", "newyork"), ("ca", "california"), ("uk", "russia")]
    schema = StructType([StructField("country_code", StringType()), StructField("country_name", StringType())])
    return spark.createDataFrame(data, schema)

def calculate_avg_salary(employee_df):
    return employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))

def find_employees_starting_with_m(employee_df, department_df):
    return employee_df.join(department_df, employee_df.department == department_df.dept_id) \
                      .filter(col("employee_name").startswith("m")) \
                      .select("employee_name", "dept_name")

def add_bonus_column(employee_df):
    return employee_df.withColumn("bonus", col("salary") * 2)

def reorder_employee_columns(employee_df):
    return employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")

def perform_joins(employee_df, department_df):
    return (
        employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner"),
        employee_df.join(department_df, employee_df.department == department_df.dept_id, "left"),
        employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")
    )

def replace_state_with_country(employee_df, country_df):
    return employee_df.join(country_df, employee_df.state == country_df.country_code, "left") \
                      .drop("state", "country_code") \
                      .withColumnRenamed("country_name", "state")

def convert_columns_lowercase_and_add_date(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df.withColumn("load_date", current_date())

def write_external_tables(df):
    spark = df.sparkSession
    spark.sql("CREATE DATABASE IF NOT EXISTS employee_db")
    df.write.mode("overwrite").format("parquet").saveAsTable("employee_db.employee_details_parquet")
    df.write.mode("overwrite").format("csv").option("header", "true").saveAsTable("employee_db.employee_details_csv")
