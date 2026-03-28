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

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

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
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

final_df = add_ingestion_date(semi_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write data to container in parquet format

# COMMAND ----------

display(final_df.filter("driver_id = 840"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.pit_stops (
# MAGIC   driver_id        INT,
# MAGIC   stop             STRING,
# MAGIC   lap              INT,
# MAGIC   time             STRING,
# MAGIC   duration         STRING,
# MAGIC   milliseconds     INT,
# MAGIC   ingestion_date   TIMESTAMP,
# MAGIC   data_source      STRING,
# MAGIC   file_date        STRING,
# MAGIC   race_id          INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC LOCATION 's3://databricks02ar/f1/processed/pit_stops'
# MAGIC

# COMMAND ----------

#There is not Unique key in the table pit_stops
"""
merge_condition = "tgt.driver_id = src.driver_id and tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')
"""

overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')


# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_processed.pit_stops group by race_id order by 1 desc;
# MAGIC --1047	23

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC