from pyspark.sql import SparkSession
from util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

def main():
    # Initialize Spark session
    spark = get_spark_session()

    # Step 1: Create DataFrame with StructType schema
    user_activity_df = create_user_activity_df(spark)

    # Step 2: Rename columns dynamically
    renamed_df = rename_columns(user_activity_df)

    # Show the renamed DataFrame
    print("Renamed DataFrame:")
    renamed_df.show(truncate=False)

    # Step 3: Calculate the number of actions by each user in the last 7 days
    actions_df = actions_last_7_days(renamed_df)
    print("Actions in last 7 days:")
    actions_df.show(truncate=False)

    # Step 4: Convert timestamp to login_date column
    login_date_df = convert_to_login_date(renamed_df)
    print("DataFrame with login_date:")
    login_date_df.show(truncate=False)

    # Step 5: Write DataFrame to CSV (update the path as per your folder structure)
    write_to_csv(login_date_df, path="output/assignment_3.csv")

    # Step 6: Write as managed table in the 'user' database
    write_to_managed_table(login_date_df)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
