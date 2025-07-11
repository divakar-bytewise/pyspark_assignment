from src.assignment_4.util import *
from pyspark.sql.functions import *
def main():
    spark = get_spark_session()

    file_path = r"nested_json_file.json"
    df = read_json(spark, file_path)

    print("Original record count:", df.count())
    flat_df = flatten_employees(df)
    print("Flattened record count:", flat_df.count())

    exploded, exploded_outer, pos_exploded = explode_examples(df, "employees")

    filtered_df = filter_by_id(df, 1001)
    snake_case_df = camel_to_snake(flat_df)

    final_df = add_load_date(snake_case_df)

    write_to_partitioned_table(final_df, output_path="output/employee_details")


    spark.stop()

if __name__ == "__main__":
    main()
