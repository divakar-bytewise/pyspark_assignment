from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, explode_outer, posexplode, to_date, current_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType

def get_spark_session(app_name="Assignment4") -> SparkSession:
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()

def read_json(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.option("multiline", "true").json("nested_json_file.json")

def flatten_employees(df: DataFrame) -> DataFrame:
    return df.withColumn("employee", explode(col("employees"))).select(
        col("id"),
        col("properties.name").alias("store_name"),
        col("properties.storeSize").alias("store_size"),
        col("employee.empId").alias("emp_id"),
        col("employee.empName").alias("emp_name")
    )

def record_counts(df: DataFrame) -> tuple:
    return df.count(), flatten_employees(df).count()

def explode_examples(df, column):
    exploded = df.select("*", explode(col(column)).alias("exploded"))
    exploded_outer = df.select("*", explode_outer(col(column)).alias("exploded_outer"))
    pos_exploded = df.select("*", posexplode(col(column)).alias("position", "pos_exploded"))
    return exploded, exploded_outer, pos_exploded

def filter_by_id(df: DataFrame, target_id: int) -> DataFrame:
    return df.filter(col("id") == target_id)

def camel_to_snake(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        snake_case = ''.join(['_' + c.lower() if c.isupper() else c for c in col_name]).lstrip('_')
        df = df.withColumnRenamed(col_name, snake_case)
    return df

def add_load_date(df: DataFrame) -> DataFrame:
    df = df.withColumn("load_date", current_date())
    return df.withColumn("year", year("load_date")) \
             .withColumn("month", month("load_date")) \
             .withColumn("day", dayofmonth("load_date"))

def write_to_partitioned_table(df: DataFrame, output_path: str):
    df.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .format("json") \
        .save(output_path)
