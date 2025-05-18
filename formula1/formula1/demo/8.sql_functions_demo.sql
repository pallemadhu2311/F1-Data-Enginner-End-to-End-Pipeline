-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### SQL functions demo

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

SELECT * FROM drivers;

-- COMMAND ----------

SELECT *, concat(driver_ref, ' - ',name) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT *,split(name," ") FROM drivers;

-- COMMAND ----------

SELECT *,split(name, ' ')[0] forename,split(name, ' ')[1] surname
FROM drivers;

-- COMMAND ----------

SELECT *, current_timestamp FROM drivers;

-- COMMAND ----------

SELECT *, date_format(dob,'dd-MM-yyyy') AS new_date_format FROM drivers;

-- COMMAND ----------

SELECT COUNT(*) FROM drivers

-- COMMAND ----------

select MAX(dob) from drivers;

-- COMMAND ----------

SELECT COUNT(*) FROM drivers 
WHERE nationality="British";

-- COMMAND ----------

SELECT nationality,COUNT(*) 
FROM drivers 
group by nationality
order by nationality;

-- COMMAND ----------

SELECT nationality,COUNT(*) 
FROM drivers 
group by nationality
HAVING count(*) > 100
order by nationality;

-- COMMAND ----------

SELECT nationality,name, dob, rank() OVER(PARTITION BY nationality ORDER BY dob) AS age_rank
FROM drivers
ORDER BY nationality,age_rank;

-- COMMAND ----------

