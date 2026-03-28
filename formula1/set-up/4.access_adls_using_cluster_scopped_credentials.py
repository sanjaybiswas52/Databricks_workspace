# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Cluster Scoped Credentials
# MAGIC <b>Azure Active Directory</b> (AAD) Credential Passthrough
# MAGIC Steps to follow:
# MAGIC 1. Set the Spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![AAD](/Volumes/devdb/raw/image/aad_credential.jpeg)
# MAGIC <h4> Same permission can be do in Unity Catalog in databricks itself.</h4>
# MAGIC <b>RBAC: </b> Role Based Access Control
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Option1
# MAGIC Need to set the key for the storage account (Azure Data storage)
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.formuladl.dfs.core.windows.net",
# MAGIC   "C24dL0hoqFxdpnx7Vq8kaBMOUWJYSBLf+oinR/GLhVtrhQRCpyDIPArCSi/Ikr/gYP5tmhQVVl++ASTaruTjg==")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Option2
# MAGIC   put this credential to Cluster then no need to use this spark.conf.set here
# MAGIC   edit clust with :
# MAGIC   <ul><li>fs.azure.account.key.formuladl.dfs.core.windows.net
# MAGIC   <li>C24dL0hoqFxdpnx7Vq8kaBMOUWJYSBLf+oinR/GLhVtrhQRCpyDIPArCSi/Ikr/gYP5tmhQVVl++ASTaruTjg==</ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using Azure Blob File System (abfs)
# MAGIC It's give full access to users.
# MAGIC <br><b>Note:</b> if use cluster scope credential then need to role (IAM) based privilege to execute below command</br>

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