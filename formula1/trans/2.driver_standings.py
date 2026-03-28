# Databricks notebook source
# MAGIC %md
# MAGIC <b>Produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True )).alias("wins"))



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc
 
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#display(driver_stangdings_df.filter("race_year = 2020"))

final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path",f"{presentation_folder_path}/driver_stangdings") \
    .saveAsTable("f1_presentation.driver_stangdings")

# COMMAND ----------

