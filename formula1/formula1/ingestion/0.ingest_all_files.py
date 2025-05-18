# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest_Races.csv_file", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest_Constructor_Json_file", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest_Drivers_File", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.Ingest_Results_file", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.Ingest_Pitstop_Multiline_Json", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.Ingest_Lap_times_file", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_Qualify_folder_Multiline_Json", 0 , {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-28"})

# COMMAND ----------

v_result