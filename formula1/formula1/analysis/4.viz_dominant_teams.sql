-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points,
      RANK() OVER(ORDER BY AVG(calucalted_points) DESC) AS team_rank
from f1_presentation.calucalted_race_results
group by team_name
HAVING count(1) >= 100
order by avg_points DESC;

-- COMMAND ----------

-- TOP 10 Drivers of all time

select 
      race_year,
      team_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
group by race_year,team_name
order by race_year,avg_points DESC;

-- COMMAND ----------

-- TOP 10 Drivers Teams  of all time

select 
      race_year,
      team_name, 
      COUNT(1) AS total_races,
      sum(calucalted_points) AS total_points,
      AVG(calucalted_points) AS avg_points
from f1_presentation.calucalted_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
group by race_year,team_name
order by race_year,avg_points DESC;