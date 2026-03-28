-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

USE CATALOG udemy

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Table for CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create circuits table
-- MAGIC <ul><li>Source is CSV file

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt DOUBLE,
  url STRING
) 
USING csv
OPTIONS (path 's3://databricks02ar/f1/raw/circuits.csv', header true);


-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create races table
-- MAGIC <ul><li>Source is CSV file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
) 
USING csv
OPTIONS (path 's3://databricks02ar/f1/raw/races.csv', header true);

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create table for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create constructors table
-- MAGIC <ul><li>Single line JSON
-- MAGIC <li>Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
) 
USING json
OPTIONS (path 's3://databricks02ar/f1/raw/constructors.json');


-- COMMAND ----------

select * from f1_raw.constructors


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create drivers table
-- MAGIC <ul><li>Single line JSON
-- MAGIC <li>Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
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
OPTIONS (path 's3://databricks02ar/f1/raw/drivers.json');

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create results table
-- MAGIC <ul><li>Single line JSON
-- MAGIC <li>Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.results;

create table if not exists f1_raw.results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText string,
  positionOrder int,
  points double,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed double,
  statusId int
)
using json
options (path 's3://databricks02ar/f1/raw/results.json', header true)


-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create pit stops table
-- MAGIC <ul><li>Multi line JSON
-- MAGIC <li>Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS (multiLine 'true')
LOCATION 's3://databricks02ar/f1/raw/pit_stops.json';

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create table for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create Lap Times Table
-- MAGIC <ul><li>Source is CSV file
-- MAGIC <li>Multiple files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;

create table if not exists f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv
options (
  path 's3://databricks02ar/f1/raw/lap_times')

-- COMMAND ----------

SELECT COUNT(*) FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <b>Create Qualifying Table
-- MAGIC <ul><li>Source is JSON file
-- MAGIC <li>Multiple Line JSON
-- MAGIC <li>Multi files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifingId INT,
  raceId INT
)
USING json
OPTIONS (multiLine 'true')
LOCATION 's3://databricks02ar/f1/raw/qualifying';

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying

-- COMMAND ----------

-- MAGIC %md
-- MAGIC