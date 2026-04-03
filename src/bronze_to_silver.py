from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("practise").getOrCreate()

BRONZE_PATH = "../data/bronze"
SILVER_PATH = "../data/silver"



customers = spark.read.parquet(f"{BRONZE_PATH}/customers")
order_items = spark.read.parquet(f"{BRONZE_PATH}/order_items")
order_payments = spark.read.parquet(f"{BRONZE_PATH}/order_payments")
order_reviews= spark.read.parquet(f"{BRONZE_PATH}/order_reviews")
orders = spark.read.parquet(f"{BRONZE_PATH}/orders")
product = spark.read.parquet(f"{BRONZE_PATH}/product")
sellers = spark.read.parquet(f"{BRONZE_PATH}/sellers")


customers_silver = customers.dropDuplicates()

order_items_silver = order_items.dropDuplicates().withColumn("shipping_limit_date",to_timestamp("shipping_limit_date"))
order_payments_silver = (order_payments.dropDuplicates().withColumn("payment_installments",col("payment_installments").cast("int")).withColumn("payment_value",col("payment_value").cast("double")))

order_reviews_silver = (
    order_reviews
    .dropDuplicates()
    .withColumn("review_score", expr("try_cast(review_score as int)"))
)
orders_silver = orders.dropDuplicates().withColumn("order_purchase_timestamp",to_timestamp("order_purchase_timestamp")).withColumn("order_approved_at",to_timestamp("order_approved_at")).withColumn("order_delivered_carrier_date",to_timestamp("order_delivered_carrier_date"))

product_silver = product.dropDuplicates(['product_id'])

sellers_silver=sellers.dropDuplicates(['seller_id'])

customers_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/customers")
order_items_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/order_items")
order_payments_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/order_payments")
order_reviews_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/order_reviews")
orders_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/orders")
product_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/product")
sellers_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}/sellers")
