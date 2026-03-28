# Databricks notebook source
# MAGIC %md
# MAGIC ###👉 Data Migration

# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹 Create single CSV file from multiple Parquet files.

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_catalog;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, current_date, spark_partition_id, countDistinct

df = spark.table("demo_catalog.demo_schema.employees")

select_df = df.select("employee_id", "employee_name", "job_title", "department_id", "location_id", "hire_date", "manager_id", "country", "salary")

addcol_df = select_df.withColumn("updated_at", current_date()) \
                     .withColumn("operation", lit("INSERT"))

# Check initial number of partitions
addcol_df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()


addcol_df.repartition(1). \
  coalesce(1) \
  .write \
  .csv("s3://sanjaydatabricks01/csv/demo/emp/employees_test.csv", header=True, mode="overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹Find Partitions in tables

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, countDistinct

df = spark.read.format('delta').load('/Volumes/demo_catalog/demo_schema/demo_raw/orders/')

df.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()

df_range = df.repartitionByRange(3, "order_date")

#print(df_range.rdd.getNumPartitions())

df_range.select(spark_partition_id().alias("pid")) \
        .agg(countDistinct("pid").alias("num_partitions")) \
        .show()


# COMMAND ----------

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
# MAGIC ####🔹Convert CSV to Delta Table

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

# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹Verify ACID transaction split files files so that count of data file will increase.

# COMMAND ----------

from delta.tables import DeltaTable

# Path to Delta table
#sales_table_path = "s3://sanjaydatabricks01/dlt/sales"
sales_table_path = "/Volumes/demo_catalog/demo_schema/demo_raw/sales/"

# Load Delta table
sales_table = DeltaTable.forPath(spark, sales_table_path)

# Example: Update records
sales_table.update(
    condition = "region = 'East'",
    set = { "amount": "amount * 1.1" }  # increase amount by 10%
)

# Example: Delete records
sales_table.delete("amount < 100")

# COMMAND ----------

# List files written
import os
files = dbutils.fs.ls("s3://sanjaydatabricks01/dlt/sales")
print("Number of parquet files:", len(files))

# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹Data migration (JSON to Delta Table)

# COMMAND ----------

df = spark.read.json("s3://sanjaydatabricks01/json/orders/")
display(df)

# Save as Delta table
df.write.format("delta").mode("overwrite").save("/Volumes/demo_catalog/demo_schema/demo_raw/orders/")


df_delta = spark.read.format("delta") \
     .load("/Volumes/demo_catalog/demo_schema/demo_raw/orders/") 


df_delta.write.format("delta").mode("overwrite").saveAsTable("demo_catalog.demo_schema.orders")


# COMMAND ----------

# MAGIC %md
# MAGIC ####🔹Data migration from Delta to Parquet file.

# COMMAND ----------

df = spark.read.format('delta').load('/Volumes/demo_catalog/demo_schema/demo_raw/sales/')
display(df)

df.write.format("parquet").mode("overwrite").save("/Volumes/demo_catalog/demo_schema/demo_raw/sales_parquet/")