from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.appName("practise").getOrCreate()

RAW_PATH = "../data/raw"
BRONZE_PATH = "../data/bronze"
files = {
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments" :"olist_order_payments_dataset.csv",
    "order_reviews":"olist_order_reviews_dataset.csv", 
    "orders": "olist_orders_dataset.csv",
    "product": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv"
}

for table_name,file_name in files.items():
    file_path = os.path.join(RAW_PATH,file_name)
    
    df = spark.read.csv(file_path,header=True,inferSchema=True)
    output_path = os.path.join(BRONZE_PATH,table_name)
    df.write.mode("overwrite").parquet(output_path)
