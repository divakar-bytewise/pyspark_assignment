from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

def create_credit_card_df(spark: SparkSession) -> DataFrame:
    data = [("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)]
    columns = StructType([StructField('card_number',StringType())])
    return spark.createDataFrame(data, columns)

def mask_card_number(card_number: str) -> str:
    return '*' * (len(card_number) - 4) + card_number[-4:]

def mask_card_number(card_number: str) -> str:
    return '*' * (len(card_number) - 4) + card_number[-4:]

# Register as UDF
mask_card_number_udf = udf(mask_card_number, StringType())

def add_masked_column(df: DataFrame) -> DataFrame:
    return df.withColumn("masked_card_number", mask_card_number_udf(col("card_number")))

def repartition_df(df: DataFrame, num_partitions: int) -> DataFrame:
    return df.repartition(num_partitions)