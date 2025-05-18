# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure data lake using the SAS Token
# MAGIC
# MAGIC ##### Steps to Follow
# MAGIC 1. Register Azure AD Application/ Service Prinicple
# MAGIC 1. Generate a Secrect / Password for the Application
# MAGIC 1. Set Spark Config with App/ Client Id / Directory / Tenet Id & Secrect
# MAGIC 1. Assign Role "Storage Blob Data Contributor" to the Data Lake

# COMMAND ----------

client_id = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXX" 
tenant_id = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXX"
client_secret = "XXXX~XX-XXXXXXXXXXXXXXXXXXXXX~"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.frml1datalakecourse.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.frml1datalakecourse.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.frml1datalakecourse.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.frml1datalakecourse.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.frml1datalakecourse.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@frml1datalakecourse.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@frml1datalakecourse.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@frml1datalakecourse.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

