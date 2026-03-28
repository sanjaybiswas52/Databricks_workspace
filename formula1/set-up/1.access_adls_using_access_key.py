# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using acsess keya
# MAGIC
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

#Need to set the key for the storage account (Azure Data storage)
spark.conf.set(
  "fs.azure.account.key.formuladl.dfs.core.windows.net",
  "C24dL0hoqFxdpnx7Vq8kaBMOUWJYSBLf+oinR/GLhVtrhQRCpyDIPArCSi/Ikr/gYP5tmhQVVl++ASTaruTjg==")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using Azure Blob File System (abfs)
# MAGIC It's give full access to users.

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net")


# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Now using "circuits.csv" from databricks location.

# COMMAND ----------

spark.read.csv("/Volumes/udemy/formula1/raw/circuits.csv").show()

# COMMAND ----------

display(spark.read.csv("/Volumes/udemy/formula1/raw/circuits.csv"))

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls /Volumes/udemy/formula1/raw

# COMMAND ----------

display(dbutils.fs.ls('/Volumes/udemy/formula1/raw'))

# COMMAND ----------

# MAGIC %md
# MAGIC