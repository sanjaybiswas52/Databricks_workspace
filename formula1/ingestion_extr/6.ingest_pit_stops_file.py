# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 1 - Read JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 2 - For multiline JSON file

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiline", True) \
    .json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Rename columns and add new columns</b>
# MAGIC
# MAGIC 1. Rename driverid and raceid
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

semi_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write data to container in parquet format

# COMMAND ----------

final_df = add_ingestion_date(semi_final_df)

#if you want to create parquet file only
#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
#display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

#if you want to create MANAGED table
#final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.pit_stops")

#if you want to create EXTERNAL table
final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path",f"{processed_folder_path}/pit_stops") \
    .saveAsTable("f1_processed.pit_stops")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC