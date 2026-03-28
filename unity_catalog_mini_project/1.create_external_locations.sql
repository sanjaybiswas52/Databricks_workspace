-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create the external locations required for this project
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

/*CREATE EXTERNAL LOCATION IF NOT EXISTS databrickscourseucextdl_bronze
 URL "abfss://bronze@databrickscourseucextdl.dfs.core.windows.net/"
 WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);
*/

-- COMMAND ----------

--DESC EXTERNAL LOCATION databrickscourseucextdl_bronze;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls "s3://databricks02ar/f1_schema/bronze/"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Catalogs and Schemas required for the project
-- MAGIC 1. Catalog - formula1_dev (Without managed location)
-- MAGIC 2. Schemas - bronze, silver and gold (With managed location)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_dev;

-- COMMAND ----------

USE CATALOG formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "s3://databricks02ar/f1_schema/bronze/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "s3://databricks02ar/f1_schema/silver/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "s3://databricks02ar/f1_schema/gold/"

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC