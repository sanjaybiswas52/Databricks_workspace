# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog udemy

# COMMAND ----------

raw_folder_path = 's3://databricks02ar/f1/raw'
processed_folder_path = 's3://databricks02ar/f1/processed'
presentation_folder_path = 's3://databricks02ar/f1/presentation'

# COMMAND ----------

raw_folder_path_extr = '/Volumes/udemy/formula1/raw'
processed_folder_path_extr = '/Volumes/udemy/formula1/processed'
presentation_folder_path_extr = '/Volumes/udemy/formula1/presentation'

# COMMAND ----------

#raw_folder_path = 'abfss://raw@formula1dl.dfs.core.windows.net'
#processed_folder_path = 'abfss://processed@formula1dl.dfs.core.windows.net'
#presentation_folder_path = 'abfss://presentation@formula1dl.dfs.core.windows.net'