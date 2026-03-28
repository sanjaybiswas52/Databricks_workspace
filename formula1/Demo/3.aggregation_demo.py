# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Aggregation function demo

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

race_results_demo = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(race_results_demo)

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, col, countDistinct

race_results_demo.select(count("race_name")).show()

# COMMAND ----------

race_results_demo.select(countDistinct("race_name")).show()

# COMMAND ----------

race_results_demo.select(sum("points")).show()
race_results_demo.select(avg("points")).show()
race_results_demo.filter(col("race_year") == 2020).select(sum("points")).show()

# COMMAND ----------

race_results_demo.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Rename column by "alias"

# COMMAND ----------

race_results_demo.groupBy("driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Rename column by "withColumnRenamed"

# COMMAND ----------

race_results_demo.groupBy("driver_name") \
    .agg(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()


# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)") 
display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df \
    .groupBy("race_year","driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) 



# COMMAND ----------

# MAGIC %md
# MAGIC <b> Use window function to partition by "race_year"

# COMMAND ----------

# DBTITLE 1,Use window function to partition by "race_year"
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

race_list = Window.partitionBy("race_year").orderBy(desc("total_points"))

demo_grouped_df.withColumn("rank", rank().over(race_list)).show(100)
