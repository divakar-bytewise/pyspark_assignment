import pytest
from pyspark.sql import SparkSession
from src.assignment_1.util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[*]").appName("PurchaseAnalysisTest").getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="module")
def purchase_data(spark):
    return create_purchase_data(spark)

@pytest.fixture(scope="module")
def product_data(spark):
    return create_product_data(spark)

def test_customer_bought_only_iphone13(purchase_data):
    result_df = customer_bought_only_iphone13(purchase_data)
    result = [row['customer'] for row in result_df.collect()]
    assert sorted(result) == [4]

def test_customer_upgraded_to_iphone14(purchase_data):
    result_df = customer_upgraded_to_iphone14(purchase_data)
    result = [row['customer'] for row in result_df.collect()]
    assert sorted(result) == [1, 3]

def test_customer_bought_all_products(purchase_data, product_data):
    result_df = customer_bought_all_products(purchase_data, product_data)
    result = [row['customer'] for row in result_df.collect()]
    assert sorted(result) == [1]
