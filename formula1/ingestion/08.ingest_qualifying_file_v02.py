# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest qualifying json files

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
# MAGIC <b>Step 1 - Read JSON file using the spark dataframe reader API

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])



# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiline", True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying/")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Rename columns and add new columns</b>
# MAGIC
# MAGIC 1. Rename driverid and raceid
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col, current_timestamp, concat_ws, when

semi_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("q1", when(col("q1") == "\\N", None).otherwise(col("q1"))) \
.withColumn("q2", when(col("q2") == "\\N", None).otherwise(col("q2"))) \
.withColumn("q3", when(col("q3") == "\\N", None).otherwise(col("q3")))


final_df = add_ingestion_date(semi_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write data to container in parquet format

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.qualifying (
# MAGIC   qualify_id      INT,
# MAGIC   race_id         INT,
# MAGIC   driver_id       INT,
# MAGIC   constructor_id  INT,
# MAGIC   number          INT,
# MAGIC   position        INT,
# MAGIC   q1              STRING,
# MAGIC   q2              STRING,
# MAGIC   q3              STRING,
# MAGIC   ingestion_date  TIMESTAMP,
# MAGIC   data_source     STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC LOCATION 's3://databricks02ar/f1/processed/qualifying'
# MAGIC

# COMMAND ----------

#There is not Unique key in the table pit_stops so used hash_id as unique key

merge_condition = "tgt.qualify_id = src.qualify_id and tgt.race_id = src.race_id"

merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

"""
overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*) FROM f1_processed.qualifying GROUP BY race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC