# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Explore DBFS root
# MAGIC
# MAGIC 1. List all the folders in the DBFS root
# MAGIC 1. Interact with the DBFS File Browser
# MAGIC 1. Upload file to DBFS root

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

