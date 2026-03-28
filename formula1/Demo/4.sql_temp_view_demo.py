# Databricks notebook source
# MAGIC %md
# MAGIC <b>Access datafrme using SQL </b>
# MAGIC <br>objective:
# MAGIC <ul><li>Create temporary views on dataframes
# MAGIC <li>Access the view from SQL cell
# MAGIC <li>Access the view from python cell

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

# createTempView is a Spark SQL function which creates a temporary view of the DataFrame only once by name "v_race_results". In next execution, it will not create the view again by the same name.

#race_results_df.createTempView("v_race_results")

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2020
# MAGIC ORDER BY points DESC
# MAGIC LIMIT 10

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Global Temporary Views</b>
# MAGIC <ul><li>Create global temporary views on dataframe
# MAGIC <li>Access the view from SQL cell
# MAGIC <li>Access the view from Python cell
# MAGIC <li>Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gv_race_results 

# COMMAND ----------

spark.sql("SELECT * FROM gv_race_results").show()