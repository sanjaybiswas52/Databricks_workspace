# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest lap_times folder

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

# MAGIC %md
# MAGIC <b>Step 1 - Read CSV file using the spark dataframe reader API

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])



# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Rename columns and add new columns</b>
# MAGIC
# MAGIC 1. Rename driverid and raceid
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

semi_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

final_df = add_ingestion_date(semi_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write data to container in parquet format

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.lap_times (
# MAGIC   race_id          INT,
# MAGIC   driver_id        INT,
# MAGIC   lap              INT,
# MAGIC   position         INT,
# MAGIC   time             STRING,
# MAGIC   milliseconds     INT,
# MAGIC   ingestion_date   TIMESTAMP,
# MAGIC   data_source      STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC LOCATION 's3://databricks02ar/f1/processed/lap_times'
# MAGIC
# MAGIC

# COMMAND ----------


#There is not Unique key in the table pit_stops
"""
merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')
"""
overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')


# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*) from f1_processed.lap_times group by race_id order by 1 desc

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC