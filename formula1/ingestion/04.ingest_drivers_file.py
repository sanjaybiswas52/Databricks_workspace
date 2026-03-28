# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 1 - Read JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                   ])


# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()


# COMMAND ----------

display(drivers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 2 - Rename columns and add new columns</b>
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

drivers_with_column_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Drop the unwanted columns</b>
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url
# MAGIC

# COMMAND ----------

dirvers_findal_df = drivers_with_column_df.drop("url")
final_df = add_ingestion_date(dirvers_findal_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write to output to processed container in parquet format</b>

# COMMAND ----------

#if you want to create parquet file only
#dirvers_findal_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
#display(spark.read.parquet(f"{processed_folder_path}/drivers"))

#if you want to create table
final_df.write.mode("overwrite") \
    .format("delta") \
    .option("path",f"{processed_folder_path}/drivers") \
    .saveAsTable("f1_processed.drivers")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")