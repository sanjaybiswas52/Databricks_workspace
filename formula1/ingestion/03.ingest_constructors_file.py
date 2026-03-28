# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 1 - Read JSON file using the spark dataframe reader

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Drop unwanted columns from dataframe

# COMMAND ----------

constructor_drop_df = constructor_df.drop("url")
display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Setp 3 - Rename colum and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructor_rename_df = constructor_drop_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

final_df = add_ingestion_date(constructor_rename_df)


# COMMAND ----------

#if you want to create parquet file only
#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
#display(dbutils.fs.ls(f"{processed_folder_path}/constructors"))

#if you want to create table
final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("path",f"{processed_folder_path}/constructors") \
    .saveAsTable("f1_processed.constructors")



# COMMAND ----------

dbutils.notebook.exit("Success")