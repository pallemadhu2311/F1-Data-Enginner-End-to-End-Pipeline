-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

