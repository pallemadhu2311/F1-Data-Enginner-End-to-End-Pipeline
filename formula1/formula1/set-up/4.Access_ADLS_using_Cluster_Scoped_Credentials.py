# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure data lake using Cluster Scoped Credentials
# MAGIC
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. List files from the demo container
# MAGIC 1. Read data from the circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@frml1datalakecourse.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@frml1datalakecourse.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

