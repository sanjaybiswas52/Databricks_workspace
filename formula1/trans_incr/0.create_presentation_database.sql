-- Databricks notebook source
USE CATALOG udemy;

-- COMMAND ----------

DROP DATABASE UDEMY.f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION "s3://databricks02ar/f1/database/presentation/"