import pytest
from src.assignment_3.util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

@pytest.fixture(scope="session")
def spark():
    return get_spark_session("TestUserActivity")

def test_create_user_activity_df(spark):
    df = create_user_activity_df(spark)
    assert df.count() == 8

def test_rename_columns(spark):
    df = create_user_activity_df(spark)
    renamed_df = rename_columns(df)
    assert "log_id" in renamed_df.columns
    assert "user_id" in renamed_df.columns
