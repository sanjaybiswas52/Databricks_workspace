# Databricks notebook source
dbutils.widgets.help()
#raw_folder_path = dbutils.widgets.get("raw_folder_path")

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sep 1 - Read the CSV file using the spark dataframe reader</b>

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), #False means that the column is not nullable
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

#circuits_df.printSchema()

#circuits_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sep 2 - Select only the required columns</b>

# COMMAND ----------

from pyspark.sql.functions import col, lit

circuits_select_df= circuits_df.select(col("circuitId"), 
                                       col("circuitRef"), 
                                       col("name"), 
                                       col("location"), 
                                       col("country"),
                                       col("lat"), 
                                       col("lng"), 
                                       col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sep 3 - Rename the columns as required</b>

# COMMAND ----------

circuits_renamed_df = circuits_select_df.withColumnRenamed("circuitId", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "latitude") \
                                        .withColumnRenamed("lng", "longitude") \
                                        .withColumnRenamed("alt", "altitude") \
                                        .withColumn("data_source", lit(v_data_source))

#circuits_renamed_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Set 4 - Add ingestion date to the dataframe</b>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

"""
circuits_final_df = circuits_renamed_df \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("env", lit("Production")) 


circuits_final_df = circuits_renamed_df \
    .withColumn("ingestion_date", current_timestamp()) 
display(circuits_final_df)
"""

circuits_final_df = add_ingestion_date(circuits_renamed_df)



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 5 - Write data to datalake as parquet</b>

# COMMAND ----------

#if you want to create parquet file only
#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits/")

#if you want to create table
circuits_final_df \
    .write.mode("overwrite") \
    .format("parquet") \
    .option("path",f"{processed_folder_path}/circuits") \
    .saveAsTable("f1_processed.circuits")



# COMMAND ----------

dbutils.notebook.exit("Success")