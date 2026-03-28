# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC   %run "../includes/configuration"  

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 1 - Read JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])



# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 2 - Rename columns and add new columns</b>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source))



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Drop the unwanted columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit

results_final_df = results_with_columns_df.drop(col("statusId"))
final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write to output to container in parquet format with partitioned by "race_id"

# COMMAND ----------

#if you want to create parquet file only
#results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")
#display(spark.read.parquet(f"{processed_folder_path}/results"))

#if you want to create table
final_df.write \
    .mode("overwrite") \
    .partitionBy("race_id") \
    .format("delta") \
    .saveAsTable("f1_processed.results")




# COMMAND ----------

dbutils.notebook.exit("Success")