-- Databricks notebook source
show databases;

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

show tables;

-- COMMAND ----------

DESC calucalted_race_results;

-- COMMAND ----------

select driver_name, sum(calucalted_points) AS total_points
from f1_presentation.calucalted_race_results
group by driver_name
order by total_points DESC
;

-- COMMAND ----------

select driver_name, COUNT(1) AS total_races,
sum(calucalted_points) AS total_points
from f1_presentation.calucalted_race_results
group by driver_name
order by total_points DESC
;

-- COMMAND ----------

select driver_name, COUNT(1) AS total_races,
sum(calucalted_points) AS total_points,
AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
group by driver_name
order by avg_points DESC
;

-- COMMAND ----------

select driver_name, COUNT(1) AS total_races,
sum(calucalted_points) AS total_points,
AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
group by driver_name
HAVING count(1) >= 50
order by avg_points DESC
;

-- COMMAND ----------

select driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE race_year BETWEEN 2001 AND 2010
group by driver_name
HAVING count(1) >= 50
order by avg_points DESC;

-- COMMAND ----------

select driver_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE race_year BETWEEN 2011 AND 2020
group by driver_name
HAVING count(1) >= 50
order by avg_points DESC;