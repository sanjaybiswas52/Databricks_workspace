# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 1 - Read the CSV file using the spark dataframe reader API</b>

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType, LongType

from pyspark.sql.functions import col, lit, to_timestamp, concat, current_timestamp

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                 ])


# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 2 - Add ingestion data and race_timestamp to the dataframe </b>

# COMMAND ----------

"""
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

"""
races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
     .withColumn("data_source", lit(v_data_source)) \
     .withColumn("file_date", lit(v_file_date))
 



# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, try_to_timestamp, current_timestamp, when

cleaned_df = races_with_timestamp_df.withColumn("time", when(col("time") == "\\N", None).otherwise(col("time")))

races_with_timestamp_df = cleaned_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", 
                to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) 



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 3 - Select only the column required & renaming as required

# COMMAND ----------

races_select_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),
                                                 col('year').alias('race_year'),
                                                 col('round'),
                                                 col('circuitId').alias('circuit_id'),
                                                 col('name'),
                                                 col('ingestion_date'),
                                                 col('race_timestamp'),
                                                 col('data_source'),
                                                 col("file_date"))
final_df = add_ingestion_date(races_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Step 4 - Write the output to processed container in parquet format

# COMMAND ----------


#if you want to create parquet file only

#final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
#display(dbutils.fs.ls(f"{processed_folder_path}/races"))

#if you want to create table

final_df.write \
    .mode("overwrite") \
    .partitionBy("race_year") \
    .format("delta") \
    .option("path",f"{processed_folder_path}/races") \
    .saveAsTable("f1_processed.races")


# COMMAND ----------

processed_folder_path

# COMMAND ----------

dbutils.notebook.exit("Success")