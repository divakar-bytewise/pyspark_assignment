import pytest
from pyspark.sql import SparkSession
from src.assignment_4.util import (
    get_spark_session,
    read_json,
    flatten_employees,
    record_counts,
    explode_examples,
    filter_by_id,
    camel_to_snake,
    add_load_date
)
from pyspark.sql.types import StructType


@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[*]").appName("TestEmployeeJson").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def json_df(spark):
    # Make sure this path is correct
    return read_json(spark, "nested_json_file.json")


def test_json_read(json_df):
    assert json_df.count() > 0, "JSON file is empty or unreadable"
    assert "employees" in json_df.columns or len(json_df.columns) > 0


def test_flatten_employees(json_df):
    flat_df = flatten_employees(json_df)
    assert flat_df.count() >= json_df.count()
    # Check if flattening removed StructType columns
    for field in flat_df.schema.fields:
        assert not isinstance(field.dataType, StructType)


def test_record_counts(json_df):
    original_count, flattened_count = record_counts(json_df)
    assert flattened_count >= original_count


def test_explode_examples(json_df):
    exploded, exploded_outer, pos_exploded = explode_examples(json_df, "employees")
    assert "exploded" in exploded.columns
    assert "exploded_outer" in exploded_outer.columns
    assert "pos_exploded" in pos_exploded.columns
    assert "position" in pos_exploded.columns


def test_filter_by_id(json_df):
    filtered_df = filter_by_id(json_df, 1001)
    # Should return either 0 or more rows but not raise an error
    assert isinstance(filtered_df.count(), int)


def test_camel_to_snake_case(json_df):
    flat_df = flatten_employees(json_df)
    snake_case_df = camel_to_snake(flat_df)
    for col_name in snake_case_df.columns:
        # Expecting lowercase with possible underscores
        assert col_name == col_name.lower()
        assert not any(c.isupper() for c in col_name)


def test_add_load_date(json_df):
    flat_df = flatten_employees(json_df)
    df_with_dates = add_load_date(flat_df)
    for col_name in ["load_date", "year", "month", "day"]:
        assert col_name in df_with_dates.columns
