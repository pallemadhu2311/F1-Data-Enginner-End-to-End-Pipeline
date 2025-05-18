-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Lesson Objectives
-- MAGIC
-- MAGIC 1. spark SQL documentation
-- MAGIC 1. create database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

use demo;

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_python;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC demo.race_results_python;

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_python

-- COMMAND ----------

SELECT * from demo.race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT * 
  FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show databases

-- COMMAND ----------

use demo;

-- COMMAND ----------

DESC EXTENDED  demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### SQL External Tables

-- COMMAND ----------

DROP TABLE demo.race_results_python_ext_py;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_ext_py;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "abfss://presentation@frml1datalakecourse.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_py;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year=2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

SELECT count(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

