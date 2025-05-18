-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
-- MAGIC     "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS f1_presentation")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC     CREATE TABLE IF NOT EXISTS f1_presentation.race_results
-- MAGIC     USING PARQUET
-- MAGIC     LOCATION '{presentation_folder_path}/race_results'
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

show databases;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_presentation;


-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

desc database f1_presentation;

-- COMMAND ----------

desc database f1_raw;

-- COMMAND ----------

SELECT * FROM f1_presentation.race_results;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.parquet(f"{presentation_folder_path}/race_results"))