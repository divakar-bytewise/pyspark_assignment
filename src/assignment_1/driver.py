from pyspark.sql import SparkSession
from util import *
    # create_purchase_data, create_product_data, customer_bought_only_iphone13, \
    # customer_upgraded_to_iphone14,\
    # customer_bought_all_products

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Purchase Analysis").getOrCreate()

    purchase_data_df = create_purchase_data(spark)
    product_data_df = create_product_data(spark)

    print("Purchase Data:")
    purchase_data_df.show()

    print("Product Data:")
    product_data_df.show()

    # 1. Customers who bought only iphone13
    print("Customers who bought only iphone13:")
    customer_bought_only_iphone13(purchase_data_df).show(truncate=False)

    # 2. Customers who upgraded from iphone13 to iphone14
    print("Customers who upgraded from iphone13 to iphone14:")
    customer_upgraded_to_iphone14(purchase_data_df).show(truncate=False)

    # 3. Customers who bought all products
    print("Customers who bought all products:")
    customer_bought_all_products(purchase_data_df, product_data_df).show(truncate=False)

