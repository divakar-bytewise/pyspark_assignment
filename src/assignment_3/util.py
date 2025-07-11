from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Set Python executables for PySpark (Windows-specific)
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

# Create Spark session with Hive support
def get_spark_session(app_name: str = "UserActivity") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

# Create the initial DataFrame
def create_user_activity_df(spark: SparkSession) -> DataFrame:
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    schema = StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)

# Dynamically rename columns
def rename_columns(df: DataFrame) -> DataFrame:
    columns_mapping = {
        "log id": "log_id",
        "user$id": "user_id",
        "action": "user_activity",
        "timestamp": "time_stamp"
    }
    for old_col, new_col in columns_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

# Actions performed by each user in the last 7 days
def actions_last_7_days(df: DataFrame) -> DataFrame:
    df = df.withColumn("time_stamp", to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss"))
    return df.filter(col("time_stamp") >= date_sub(current_timestamp(), 7)) \
             .groupBy("user_id").count()

# Convert timestamp to login_date column with date type
def convert_to_login_date(df: DataFrame) -> DataFrame:
    return df.withColumn("time_stamp", to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss")) \
             .withColumn("login_date", to_date(col("time_stamp")))

# Write to CSV (use the correct folder path)
def write_to_csv(df: DataFrame, path: str) -> None:
    df.write.option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .csv("output/assignment_3.csv")

# Write as a managed Hive table
def write_to_managed_table(df: DataFrame) -> None:
    spark = df.sparkSession
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    df.write.mode("overwrite").saveAsTable("user.login_details")
