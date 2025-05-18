# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure data lake using the SAS Token
# MAGIC 1. set the spark config for SAS Token
# MAGIC 1. List files from the demo container
# MAGIC 1. Read data from the circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.frml1datalakecourse.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.frml1datalakecourse.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.frml1datalakecourse.dfs.core.windows.net", "sp=rl&st=2025-05-04T08:19:06Z&se=2025-05-04T16:19:06Z&spr=https&sv=2024-11-04&sr=c&sig=IuiEYgUWosxwbKbfOdHmYkncZT09khLMZA5JVu2jKTM%3D")


# COMMAND ----------

dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@frml1datalakecourse.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@frml1datalakecourse.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

