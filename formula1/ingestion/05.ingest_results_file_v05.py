# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

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
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Drop the unwanted columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit

results_columns_drop_df = results_with_columns_df.drop(col("statusId"))
results_final_df = add_ingestion_date(results_columns_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write to output to container in parquet format with partitioned by "race_id"

# COMMAND ----------

# Set overwrite mode for dynamic partitions
"""
Dynamic: ensures that when you overwrite data in a partitioned table, only the partitions containing new data are replaced, instead of wiping out all partitions. This prevents accidental data loss and makes overwrites more efficient.

Note partition column should be at last in the select statement
"""
#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #### For Dynamic Partition wise incremental load
# MAGIC <b>Note :</b> partition column should be at last in the select statement hence there are 2 method
# MAGIC <ul><lo><b>Method 1 :</b> Dynamically re-arrenge column and place partition column at last
# MAGIC <lo><b>Method 2:</b> Hard code columns before incremental load. 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.results (
# MAGIC   result_id          INT,
# MAGIC   race_id            INT,
# MAGIC   driver_id          INT,
# MAGIC   constructor_id     INT,
# MAGIC   number             INT,
# MAGIC   grid               INT,
# MAGIC   position           INT,
# MAGIC   position_text      STRING,
# MAGIC   position_order     INT,
# MAGIC   points             FLOAT,
# MAGIC   laps               INT,
# MAGIC   time               STRING,
# MAGIC   milliseconds       INT,
# MAGIC   fastest_lap        INT,
# MAGIC   rank               INT,
# MAGIC   fastest_lap_time   STRING,
# MAGIC   fastest_lap_speed  FLOAT,
# MAGIC   ingestion_date     TIMESTAMP,
# MAGIC   data_source        STRING,
# MAGIC   file_date          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC LOCATION 's3://databricks02ar/f1/processed/results'

# COMMAND ----------

# DBTITLE 1,Write or merge results to Delta table
#spark.confi.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

from delta.tables import DeltaTable

if (spark.catalog.tableExists("f1_processed.results")):
    #deltaTable = DeltaTable.forName(spark, "f1_processed.results")
    deltaTable = DeltaTable.forPath(spark, "s3://databricks02ar/f1/processed/results/")
    deltaTable.alias("tgt").merge(
        results_final_df.alias("src"),
        "tgt.result_id = src.result_id and tgt.race_id = src.race_id" \
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    results_final_df.write.mode("overwrite") \
        .partitionBy("race_id") \
        .format("delta") \
        .saveAsTable("f1_processed.results")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) 
# MAGIC FROM f1_processed.results
# MAGIC --WHERE race_id = 1052
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")