-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("DROP DATABASE IF EXISTS f1_processed CASCADE")
-- MAGIC spark.sql("""
-- MAGIC     CREATE DATABASE IF NOT EXISTS f1_processed
-- MAGIC     LOCATION '/mnt/formula1/processed'
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("DROP DATABASE IF EXISTS f1_presentation CASCADE")
-- MAGIC spark.sql("""
-- MAGIC     CREATE DATABASE IF NOT EXISTS f1_presentation
-- MAGIC     LOCATION '/mnt/formula1/presentation'
-- MAGIC """)