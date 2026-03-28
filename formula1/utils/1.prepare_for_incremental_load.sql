-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Drop all tables

-- COMMAND ----------

use catalog udemy;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
MANAGED LOCATION "s3://databricks02ar/f1/database/processed/"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION "s3://databricks02ar/f1/database/presentation/"