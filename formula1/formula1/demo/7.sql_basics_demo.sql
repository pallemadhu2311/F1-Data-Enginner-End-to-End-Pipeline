-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

use f1_processed;


-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM circuits

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * FROM drivers LIMIT 10;

-- COMMAND ----------

SELECT * FROM drivers WHERE nationality = 'British';

-- COMMAND ----------

SELECT * FROM drivers WHERE nationality = 'British' AND dob > '1990-01-01';

-- COMMAND ----------

SELECT name,dob FROM drivers WHERE nationality = 'British' AND dob > '1990-01-01';

-- COMMAND ----------

SELECT name,dob AS data_of_birth FROM drivers WHERE nationality = 'British' AND dob > '1990-01-01';

-- COMMAND ----------

SELECT name,dob AS data_of_birth 
FROM drivers 
WHERE nationality = 'British' AND dob > '1990-01-01' 
ORDER BY data_of_birth;

-- COMMAND ----------

SELECT name,dob AS data_of_birth 
FROM drivers 
WHERE (nationality = 'British' AND dob > '1990-01-01') OR (nationality = 'Indian') 
ORDER BY data_of_birth DESC;

-- COMMAND ----------

