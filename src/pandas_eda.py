import pandas as pd
import matplotlib.pyplot as plt
order = pd.read_csv("../data/raw/olist_orders_dataset.csv")
order_items = pd.read_csv("../data/raw/olist_order_items_dataset.csv")
reviews = pd.read_csv("../data/raw/olist_order_reviews_dataset.csv")

# print("orders shape",order.shape)
# print("order_items",order_items.shape)
# print("reviews",reviews.shape)

# print(order.columns)
# print("Missiing")
# print(order.isnull().sum())

date_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for i in date_cols:
    order[i]=pd.to_datetime(order[i],errors="coerce")
print(order.dtypes)


order['delay_days'] = (
   order["order_estimated_delivery_date"] - order['order_delivered_customer_date']
).dt.days


print(order['delay_days'].describe())

order['year_month']=order['order_purchase_timestamp'].dt.to_period("M").astype(str)
monthly_orders = order.groupby("year_month").size().reset_index(name="total_orders")

# print(monthly_orders.head())

plt.figure(figsize=(12,5))
plt.plot(monthly_orders["year_month"], monthly_orders["total_orders"])
plt.xticks(rotation=90)
plt.xlabel("Month")
plt.ylabel("Total Orders")
plt.title("Monthly Orders Trend")
plt.grid() 
plt.show()
