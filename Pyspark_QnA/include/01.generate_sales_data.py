# Databricks notebook source
# MAGIC %md
# MAGIC ####🔹Generate "sales" CSV file

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG demo_catalog;
# MAGIC
# MAGIC drop table demo_catalog.demo_schema.sales;
# MAGIC

# COMMAND ----------

import random
import csv
from datetime import datetime, timedelta

# Generate 100 records
start_date = datetime(2025, 1, 1)
regions = ["North", "South", "East", "West"]

with open("/Volumes/demo_catalog/demo_schema/demo_raw/sales_data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_date","customer_id","product_id","region","amount"])
    
    for i in range(100):
        order_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        customer_id = f"C{str(i+1).zfill(3)}"
        product_id = f"P{100+i}"
        region = regions[i % 4]
        amount = round(random.uniform(50, 500), 2)
        writer.writerow([order_date, customer_id, product_id, region, amount])


# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹Split file.

# COMMAND ----------

# Read CSV file
df = spark.read.csv("/Volumes/demo_catalog/demo_schema/demo_raw/sales_data.csv", header=True, inferSchema=True)

# Check schema
df.printSchema()

# Repartition so each partition has exactly one row
row_count = df.count()
df_single = df.repartition(row_count)

# Save as Delta table
df_single.write.format("delta").mode("overwrite").save("s3://sanjaydatabricks01/dlt/sales")

# Save as Delta table
#df.write.format("delta").mode("overwrite").saveAsTable("demo_catalog.demo_schema.user_trans")

# Register Delta table from location
spark.sql("""
CREATE TABLE if not exists demo_catalog.demo_schema.sales
USING DELTA
LOCATION 's3://sanjaydatabricks01/dlt/sales'
""")