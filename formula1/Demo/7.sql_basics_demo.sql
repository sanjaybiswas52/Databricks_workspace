-- Databricks notebook source
use catalog udemy

-- COMMAND ----------

-- MAGIC %sql show databases;

-- COMMAND ----------

SELECT driver_id, driver_ref, name, 
split(name, ' '),
split(name, ' ')[0] forename,
split(name, ' ')[1] sirname
FROM f1_processed.drivers