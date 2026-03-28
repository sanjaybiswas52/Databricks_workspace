-- Databricks notebook source
use catalog udemy;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_dominent_drivers
AS
SELECT driver_name,
       COUNT(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       rank() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT race_year,
       driver_name,
       COUNT(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominent_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;
