# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure data lake using the access keys
# MAGIC
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. List files from the demo container
# MAGIC 1. Read data from the circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<storageAccount>.dfs.core.windows.net",
    "<Storage-Key>"
)

# COMMAND ----------

dbutils.fs.ls("abfss://<containter-name>@<StorageAccount>.dfs.core.windows.net")

# COMMAND ----------

