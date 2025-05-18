-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Circuits TABLE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
-- MAGIC     "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
-- MAGIC )
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/circuits.csv",header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create races TABLE

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/races.csv",header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create constructor JSON table
-- MAGIC
-- MAGIC 1. Single Line JSON
-- MAGIC 1. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Drivers JSON table
-- MAGIC
-- MAGIC 1. Single Line JSON
-- MAGIC 1. Complex Structure (forename + surname)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Results JSON table
-- MAGIC
-- MAGIC 1. Single Line JSON
-- MAGIC 1. Complex Structure 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed DOUBLE,
  statusId STRING
)
USING json
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create pit_stops TABLE from JSON file
-- MAGIC
-- MAGIC 1. Single Line JSON
-- MAGIC 1. Complex Structure 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/pit_stops.json", multiLine True)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create TABLES list of files
-- MAGIC
-- MAGIC ##### Create Lap Times table
-- MAGIC
-- MAGIC 1. CSV file
-- MAGIC 1. Multiple files in Folder

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  time STRING,
  position INT,
  milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create TABLES list of files
-- MAGIC
-- MAGIC ##### Create Qualifying table
-- MAGIC
-- MAGIC 1. JSON file
-- MAGIC 1. MultiLine JSON
-- MAGIC 1. Multiple Lines

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
USING json
OPTIONS (path "abfss://raw@frml1datalakecourse.dfs.core.windows.net/qualifying", multiLine True)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

