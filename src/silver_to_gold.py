from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("practise").getOrCreate()

SILVER_PATH = "../data/silver"
GOLD_PATH = "../data/gold"



customers = spark.read.parquet(f"{SILVER_PATH}/customers")
order_items = spark.read.parquet(f"{SILVER_PATH}/order_items")
order_payments = spark.read.parquet(f"{SILVER_PATH}/order_payments")
order_reviews = spark.read.parquet(f"{SILVER_PATH}/order_reviews")
orders = spark.read.parquet(f"{SILVER_PATH}/orders")
product = spark.read.parquet(f"{SILVER_PATH}/product")
sellers = spark.read.parquet(f"{SILVER_PATH}/sellers")

# customers.printSchema()
# order_items.printSchema()
# order_payments.printSchema()
# order_reviews.printSchema()
# orders.printSchema()
# product.printSchema()
# sellers.printSchema()

dim_customers = customers.dropDuplicates(['customer_id'])

dim_product = product.dropDuplicates(['product_id'])

dim_sellers = sellers.dropDuplicates(['seller_id'])

fact_orders = (
    order_items
    .join(orders,"order_id","left")
    .join(order_payments.groupBy("order_id").agg(sum("payment_value").alias("total_payments")),"order_id","left")
    .join(order_reviews.select("order_id","review_score"),"order_id","left")
    .withColumn("delivery_days",date_diff("order_delivered_customer_date","order_purchase_timestamp"))
    .withColumn("estimated_days",date_diff("order_estimated_delivery_date","order_purchase_timestamp"))
    .withColumn("delay_days",datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")))
    .withColumn("is_delayed",when(col("delay_days")>0,1).otherwise(0))

)
# Daily sales mart
mart_daily_sales = (
    fact_orders
    .groupBy("order_purchase_timestamp")
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("price").alias("avg_order_item_value")
    )
)
fact_orders.printSchema()

# Category performance mart
mart_category_performance = (
    fact_orders
    .join(dim_product, "product_id", "left")
    .groupBy("product_category_name")
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("review_score").alias("avg_review_score"),
        avg("delivery_days").alias("avg_delivery_days")
    )
)

# Seller performance mart
mart_seller_performance = (
    fact_orders
    .groupBy("seller_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("freight_value").alias("avg_freight_value"),
        avg("review_score").alias("avg_review_score"),
        avg("delay_days").alias("avg_delay_days")
    )
)

# Customer mart
customer_orders = (
    fact_orders
    .join(dim_customers, "customer_id", "left")
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("customer_lifetime_value"),
        avg("review_score").alias("avg_review_score")
    )
)

dim_customers.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customers")
dim_sellers.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_sellers")
dim_product.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_products")
fact_orders.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_orders")
mart_daily_sales.write.mode("overwrite").parquet(f"{GOLD_PATH}/mart_daily_sales")
mart_category_performance.write.mode("overwrite").parquet(f"{GOLD_PATH}/mart_category_performance")
mart_seller_performance.write.mode("overwrite").parquet(f"{GOLD_PATH}/mart_seller_performance")
customer_orders.write.mode("overwrite").parquet(f"{GOLD_PATH}/mart_customer_summary")

# fact_orders.printSchema()
# fact_orders.filter(col("is_delayed")==1).show()













