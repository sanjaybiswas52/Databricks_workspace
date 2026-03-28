# Databricks notebook source
# MAGIC %md
# MAGIC <b>Produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_stangdings_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True )).alias("wins"))



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc
 
constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_stangdings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.printSchema())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.constructor_stangdings (
# MAGIC   race_year     INT,
# MAGIC   team          STRING,
# MAGIC   total_points  DOUBLE,
# MAGIC   wins          BIGINT,
# MAGIC   rank          INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks02ar/f1/presentation/constructor_stangdings'
# MAGIC

# COMMAND ----------


merge_condition = "tgt.team = src.team and tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_stangdings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_stangdings

# COMMAND ----------

