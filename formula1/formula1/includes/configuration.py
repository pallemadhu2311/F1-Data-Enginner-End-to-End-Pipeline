# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

demo = 'abfss://demo@frml1datalakecourse.dfs.core.windows.net'
raw_folder_path = 'abfss://raw@frml1datalakecourse.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@frml1datalakecourse.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@frml1datalakecourse.dfs.core.windows.net'
