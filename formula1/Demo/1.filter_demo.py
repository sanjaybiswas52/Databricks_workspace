# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.get('processed_folder_path')

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")


# COMMAND ----------

# SQL Syntax
races_filter_df = race_df.filter("race_year == 2019 and round <= 5")

# COMMAND ----------

# Python Syntax
races_filter_df = races_df.filter((races_df['race_year'] == 2019) & (races_df['round'] <= 5))

# COMMAND ----------

# Python Syntax
races_filter_df = races_df.where((races_df['race_year'] == 2019) & (races_df['round'] <= 5))

# COMMAND ----------

display(races_filter_df)