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
# MAGIC ####Step 4 - Write to output to container in parquet format with partitioned by "race_id"
# MAGIC <b>Set overwrite mode for dynamic partitions</b>
# MAGIC
# MAGIC <b>Dynamic:</b> ensures that when you overwrite data in a partitioned table, only the partitions containing new data are replaced, instead of wiping out all partitions. This prevents accidental data loss and makes overwrites more efficient.
# MAGIC
# MAGIC Note partition column should be at last in the select statement

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

partition_column = 'race_id'
column_list = []
for column_name in results_final_df.schema.names:
    if column_name != partition_column:
        column_list.append(column_name)
column_list.append(partition_column)
 
print(column_list)
output_df = results_final_df.select(column_list)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2

# COMMAND ----------

# Select relevant columns from results_final_df
"""
results_final_df = results_final_df.select(
    "result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps" "time", "milliseconds","fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id"
)
"""

# COMMAND ----------


# Conditional save logic
#output_df.write.mode("overwrite").insertInto("f1_processed.results")

output_df.write \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .insertInto("f1_processed.results")

"""
if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
else: 
output_df.write.mode("overwrite") \
    .partitionBy("race_id") \
    .format("parquet") \
    .option("path",f"{processed_folder_path}/results") \
    .option("replaceWhere", "race_id = 1234") \
    .saveAsTable("f1_processed.results")
"""
#output_df.createOrReplaceTempView("results_final_df")


# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC MERGE INTO f1_processed.results AS tgt
# MAGIC USING results_final_df AS src
# MAGIC ON tgt.result_id = src.result_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT result_id, count(1) 
# MAGIC FROM f1_processed.results
# MAGIC --WHERE race_id = 1052
# MAGIC GROUP BY result_id
# MAGIC ORDER BY count(1) DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")