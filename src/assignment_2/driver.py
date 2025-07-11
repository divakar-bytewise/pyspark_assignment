from pyspark.sql import SparkSession
from util import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark = SparkSession.builder.master("local[*]").appName("CreditCardMasking").getOrCreate()

# 1. Create DataFrame
credit_card_df = create_credit_card_df(spark)
credit_card_df.show(truncate=False)

# 2. Print original partition count
print("Original partitions:", credit_card_df.rdd.getNumPartitions())

# 3. Increase partition count to 5
repartitioned_df = repartition_df(credit_card_df, 20)
print("Increased partitions:", repartitioned_df.rdd.getNumPartitions())

# 4. Decrease back to original (default 1)
original_partition_df = repartition_df(repartitioned_df, 1)
print("Reduced partitions:", original_partition_df.rdd.getNumPartitions())

# 5 & 6. Add masked column and display
masked_df = add_masked_column(original_partition_df)
masked_df.show(truncate=False)

spark.stop()
