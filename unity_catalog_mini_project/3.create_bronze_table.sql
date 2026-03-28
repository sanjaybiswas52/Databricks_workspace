-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Create Bronze Tables
-- MAGIC <b>drivers.json</b>
-- MAGIC <br><b>results.json</b></br>
-- MAGIC Bronze folder path - s3://databricks02ar/f1_schema/formul1_schema/bronze/

-- COMMAND ----------

USE CATALOG formula1_dev;

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.drivers
(
    driverId INT,
    driverRef STRING,
    number INT,
    code STRING,
    name STRUCT<forename: STRING, surname: STRING>,
    dob DATE,
    nationality STRING,
    url STRING
)
USING json
OPTIONS (path "s3://databricks02ar/f1_schema/bronze/drivers.json");

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.results;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.results
(
    resultId INT,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,grid INT,
    position INT,
    positionText STRING,
    positionOrder INT,
    points INT,
    laps INT,
    time STRING,
    milliseconds INT,
    fastestLap INT,
    rank INT,
    fastestLapTime STRING,
    fastestLapSpeed FLOAT,
    statusId STRING
)
USING json
OPTIONS (path "s3://databricks02ar/f1_schema/bronze/results.json");