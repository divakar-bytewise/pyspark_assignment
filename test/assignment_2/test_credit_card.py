import pytest
from pyspark.sql import SparkSession
from src.assignment_2.util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[*]").appName("TestCreditCard").getOrCreate()
    yield spark_session
    spark_session.stop()


def test_mask_card_number():
    assert mask_card_number("1234567812345678") == "************5678"


def test_add_masked_column(spark):
    df = create_credit_card_df(spark)
    masked_df = add_masked_column(df)
    results = masked_df.select("masked_card_number").rdd.flatMap(lambda x: x).collect()

    for masked in results:
        assert masked.endswith(masked[-4:])
        assert len(masked.replace('*', '')) == 4
