-- Databricks notebook source
show databases;

-- COMMAND ----------

use f1_presentation;


-- COMMAND ----------

select 
      team_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
group by team_name
HAVING count(1) >= 100
order by avg_points DESC
;

-- COMMAND ----------

select 
      team_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE race_year BETWEEN 2001 AND 2011
group by team_name
HAVING count(1) >= 100
order by avg_points DESC
;

-- COMMAND ----------

select 
      team_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE race_year BETWEEN 2011 AND 2020
group by team_name
HAVING count(1) >= 100
order by avg_points DESC
;