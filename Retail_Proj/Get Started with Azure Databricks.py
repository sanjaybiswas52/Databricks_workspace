# Databricks notebook source
# MAGIC %md
# MAGIC ### List of files and folders

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls /Volumes/retail_db/raw/orders

# COMMAND ----------

df = spark.read.csv(
    '/Volumes/retail_catalog/retail_db/raw/orders/',
    header=True,
    inferSchema=True
)

""" OR """

orders = spark.read.csv(
    '/Volumes/retail_catalog/retail_db/raw/orders/',
    schema='order_id INT, order_date TIMESTAMP, order_customer_id INT, order_status STRING'
)

orders.show()

# COMMAND ----------

df.count()

# COMMAND ----------

import pyspark.sql.functions as F

orders.groupBy('order_date'). \
    agg(F.count('*').alias('total_orders')). \
    show()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

orders.select("order_date", F.count("*").over(Window.partitionBy("order_date")).alias("total_orders")).show()

# COMMAND ----------

orders.groupBy('order_date'). \
    agg(F.count('*').alias('total_orders')). \
    write. \
    mode('overwrite'). \
    csv('/Volumes/retail_catalog/retail_db/raw/orders_count_by_date/', header=True)

# COMMAND ----------

# MAGIC %fs ls /Volumes/retail_catalog/retail_db/raw/orders_count_by_date/

# COMMAND ----------

order_count_by_date=spark.read.csv('/Volumes/retail_catalog/retail_db/raw/orders_count_by_date', header=True).  \
    show()

# COMMAND ----------

order_items=spark.read.csv('/Volumes/retail_catalog/retail_db/raw/order_items/', \
    schema='order_item_id INT, order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT'
    )

# COMMAND ----------

daily_revenue=orders. \
    filter('order_status in ("COMPLETE", "CLOSED")'). \
    join(order_items, orders['order_id'] == order_items['order_item_order_id'])


# COMMAND ----------

display(daily_revenue)


# COMMAND ----------

from pyspark.sql.functions import sum

daily_revenue=orders. \
    filter('order_status in ("COMPLETE", "CLOSED")'). \
    join(order_items, orders['order_id'] == order_items['order_item_order_id']). \
    groupBy('order_date'). \
    agg(sum('order_item_subtotal').alias('revenue'))
display(daily_revenue)


# COMMAND ----------

from pyspark.sql.functions import sum, round

daily_revenue=orders. \
    filter('order_status in ("COMPLETE", "CLOSED")'). \
    join(order_items, orders['order_id'] == order_items['order_item_order_id']). \
    groupBy('order_date'). \
    agg(round(sum('order_item_subtotal'), 2).alias('revenue'))
display(daily_revenue)
