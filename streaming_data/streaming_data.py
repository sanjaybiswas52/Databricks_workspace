# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("CheckpointingWithFile").getOrCreate()

# Define schema explicitly
schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("txn_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Define streaming source: CSV files arriving in a folder
input_stream = (
    spark.readStream
         .option("header", "true")
         .schema(schema)   # <-- Explicit schema required
         .csv("/Volumes/demo_catalog/demo_schema/demo_raw/stream_data/transactions/")
)

# Transform: simple aggregation by transaction type
agg_stream = (
    input_stream.groupBy("txn_type")
                .agg(sum("amount").alias("total_amount"))
)

# Write stream with checkpointing
query = (
    agg_stream.writeStream
              .format("delta")
              .option("checkpointLocation", "/Volumes/demo_catalog/demo_schema/demo_raw/stream_data/checkpoints/")
              .option("path", "/Volumes/demo_catalog/demo_schema/demo_raw/stream_data/delta/transactions_summary/")
              .outputMode("complete")
              .start()
)

#query.awaitTermination()
