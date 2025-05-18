-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel;">Report On Dominant Formula 1 Drivers</h1> """
-- MAGIC
-- MAGIC displayHTML(html)

-- COMMAND ----------

select driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
group by driver_name
HAVING count(1) >= 50
order by avg_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points,
      RANK() OVER(ORDER BY AVG(calucalted_points) DESC) AS driver_rank
from f1_presentation.calucalted_race_results
group by driver_name
HAVING count(1) >= 50
order by avg_points DESC;

-- COMMAND ----------

-- TOP 10 Drivers of all time

select 
      race_year,
      driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
group by race_year,driver_name
order by race_year,avg_points DESC;

-- COMMAND ----------

-- TOP 10 Drivers of all time

select 
      race_year,
      driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
group by race_year,driver_name
order by race_year,avg_points DESC;

-- COMMAND ----------

-- TOP 10 Drivers of all time

select 
      race_year,
      driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
group by race_year,driver_name
order by race_year,avg_points DESC;