# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest qualifying json files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

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
    .json(f"{raw_folder_path}/qualifying/")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Rename columns and add new columns</b>
# MAGIC
# MAGIC 1. Rename driverid and raceid
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

semi_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write data to container in parquet format

# COMMAND ----------

final_df = add_ingestion_date(semi_final_df)

#if you want to create parquet file only
#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
#display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

#if you want to create MANAGED table
final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.qualifying")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC