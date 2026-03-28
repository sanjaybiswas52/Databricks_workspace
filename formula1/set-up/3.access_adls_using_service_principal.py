# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal
# MAGIC Steps to follow:
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 4. Assign Role Storage Blob Data Contributor to the Data Lake
# MAGIC

# COMMAND ----------

client_id = "33797992-e8e8-44fe-9d57-9a9c703b0198"
tenant_id = "65947afd-bdbe-440f-b3d5-2ca66af0ef41"
client_secret = "KwF8Q-dL-tgT1Siqp3.PWNQVmGnFRMRm60aCId17"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sample code : available in 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token
# MAGIC
# MAGIC service_credential = dbutils.secrets.get(scope="&lt;cope&gt;", key="&lt;service-credential-key&gt;")
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.&lt;storage-account&gt;.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.&lt;storage-account&gt;.dfs.core.windows.net", 
# MAGIC                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.&lt;storage-account&gt;.dfs.core.windows.net", "&lt;application-id&gt;")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.&lt;storage-account&gt;.dfs.core.windows.net", service_credential)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.&lt;storage-account&gt;.dfs.core.windows.net", 
# MAGIC                "https://login.microsoftonline.com/&lt;directory-id&gt;/oauth2/token")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>>STeps for client_id and Directory(tenent ID):</b>
# MAGIC Search "azure active directory"
# MAGIC app registration
# MAGIC new registration
# MAGIC <br><b>>STeps for secret key and value:</b>
# MAGIC go to Azure cloud storage (Home>Default Directory>App Registration>formula1-app)
# MAGIC certificate & secrets
# MAGIC Create New Scerets
# MAGIC copy scret value.</br>

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

# MAGIC %md
# MAGIC