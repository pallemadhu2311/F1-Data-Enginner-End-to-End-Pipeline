# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure data lake using the access keys
# MAGIC
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. List files from the demo container
# MAGIC 1. Read data from the circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net")

# COMMAND ----------

