-- Databricks notebook source
USE CATALOG udemy

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####If you will not share location then by default it will create databricks itself

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
MANAGED LOCATION "s3://databricks02ar/f1/database/processed/"

-- COMMAND ----------

desc database f1_processed