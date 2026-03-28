-- Databricks notebook source
USE CATALOG udemy;
USE DATABASE f1_processed;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
USING parquet
LOCATION 's3://databricks02ar/f1/presentation1/calculated_race_results'
AS
SELECT races.race_year,
       constructors.name as team_name,
       drivers.name as driver_name,
       results.position,
       results.points,
       11 - results.position as calculated_points
FROM f1_processed.results
join f1_processed.drivers on (results.driver_id = drivers.driver_id)
join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
join f1_processed.races on (results.race_id = races.race_id)
WHERE results.position <= 10;


-- COMMAND ----------

INSERT OVERWRITE TABLE f1_presentation.calculated_race_results
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
FROM f1_processed.results
JOIN f1_processed.drivers ON results.driver_id = drivers.driver_id
JOIN f1_processed.constructors ON results.constructor_id = constructors.constructor_id
JOIN f1_processed.races ON results.race_id = races.race_id
WHERE results.position <= 10;


-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results;