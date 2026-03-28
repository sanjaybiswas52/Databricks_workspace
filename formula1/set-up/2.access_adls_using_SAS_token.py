# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS Token - (Shared Access Signatures)
# MAGIC Give required access for users example need to give only for read access with limited period of time.
# MAGIC 1. Set the spark config SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sample code: Go to Azure storage container and generate SAS key</b>
# MAGIC <ul><li>spark.conf.set("fs.azure.account.auth.type.&lt;storage-account&gt;."dfs.core.windows.net", "SAS")
# MAGIC <li>spark.conf.set("fs.azure.sas.token.provider.type.&lt;storage-account&gt;.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# MAGIC <li>spark.conf.set("fs.azure.sas.fixed.token.&lt;storage-account&gt;.dfs.core.windows.net", "&lt;token&gt;")</li></ul>

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1;.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1.dfs.core.windows.net", "dhjfjdadjfdsjds")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using Azure Blob File System (abfs)

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

# MAGIC %md
# MAGIC