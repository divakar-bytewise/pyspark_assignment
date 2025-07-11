from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

def create_purchase_data(spark:SparkSession) -> DataFrame:

 data=[(1, "iphone13"),
  (1, "dell i5 core"),
  (2, "iphone13"),
  (2, "dell i5 core"),
  (3, "iphone13"),
  (3, "dell i5 core"),
  (1, "dell i3 core"),
  (1, "hp i5 core"),
  (1, "iphone14"),
  (3, "iphone14"),
  (4, "iphone13")
 ]
 schema=StructType([StructField('customer',IntegerType()),
                    StructField('product_model',StringType())])

 return spark.createDataFrame(data,schema)

def create_product_data(spark:SparkSession)-> DataFrame:
 data1=[("iphone13",),
 ("dell i5 core",),
  ("dell i3 core",),
  ("hp i5 core",),
  ("iphone14",)
 ]
 schema1=StructType([StructField('product_model',StringType())])
 return spark.createDataFrame(data1,schema1)

def customer_bought_only_iphone13(purchase_data_df:DataFrame)-> DataFrame:
 return purchase_data_df.groupby('customer').agg(collect_set('product_model').alias('product')).\
  filter(array_size('product')==1).filter(array_contains(col('product'),'iphone13'))

def customer_upgraded_to_iphone14(purchase_data_df:DataFrame)->DataFrame:
 df=purchase_data_df.groupby('customer').agg(collect_set('product_model').alias('product'))
 return df.filter(array_contains(df.product,'iphone13') &
                  (array_contains(df.product,'iphone14')))

def customer_bought_all_products(purchase_data_df:DataFrame,product_data_df:DataFrame)->DataFrame:
 all_products=[row['product_model'] for row in product_data_df.collect()]

 df=purchase_data_df.groupby('customer').agg(collect_set('product_model').alias('product')).filter\
  (size(array_except(array(*[lit(p) for p in all_products]),col('product')))==0)
 return df

