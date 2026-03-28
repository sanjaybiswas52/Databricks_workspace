-- Databricks notebook source
-- MAGIC %md
-- MAGIC <b>Learning Objectives</b>
-- MAGIC <ol><li>Spark SQL Documentation
-- MAGIC <li>Create database Demo
-- MAGIC <li>Data tab in UI
-- MAGIC <li>SHOW command
-- MAGIC <li>DESCRIBE command
-- MAGIC <li>Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Udemy.demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

USE CATALOG Udemy;
USE DATABASE demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT CURRENT_CATALOG(), CURRENT_SCHEMA(), CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES IN default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Learning Objectives</b>
-- MAGIC <ol><li>Create manage table using Python
-- MAGIC <li>Create manage table using SQL
-- MAGIC <li>Effect of dropping of managed table
-- MAGIC <li>Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

USE CATALOG udemy;
USE DATABASE demo;

-- COMMAND ----------

SELECT current_catalog(), current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #	•	mode(“ignore”)
-- MAGIC #	•	If table does not exist → table will be created
-- MAGIC #	•	If table already exists → nothing happens (no error)
-- MAGIC
-- MAGIC #race_results_df.write.format("parquet").saveAsTable("race_results_python")
-- MAGIC race_results_df.write.format("delta").mode("ignore").saveAsTable("race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC race_results_python

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python

-- COMMAND ----------

drop database if exists formula1_demodb cascade


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Table created by SQL

-- COMMAND ----------

EXPLAIN
SELECT *
FROM demo.race_results_sql
WHERE position = 1;

-- COMMAND ----------

EXPLAIN FORMATTED
SELECT *
FROM demo.race_results_sql
WHERE position = 1;

-- COMMAND ----------

EXPLAIN COST
SELECT *
FROM demo.race_results_sql;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED race_results_sql