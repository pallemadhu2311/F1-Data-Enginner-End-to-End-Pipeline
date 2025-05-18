# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest the Races.CSV file
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 1. Read the CSV file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_data

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://raw@frml1datalakecourse.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Add ingestion date & race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, IntegerType, StructType, DataType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", StringType(), True),
                                   StructField("time", StringType(), True)
 ])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_data}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,col,lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingeston_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn("data_source", lit(v_data_source)) \
                                  .withColumn("file_date", lit(v_file_data))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 3. Select only the required columns & Rename as Required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_Id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_Id'),col('name'),col('ingeston_date'),col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 4. Write the output to processed container in parquet format

# COMMAND ----------

dbutils.fs.rm("abfss://processed@frml1datalakecourse.dfs.core.windows.net/races/_delta_log", recurse=True)

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

races_selected_df.write.mode("overwrite").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------


display(dbutils.fs.ls("abfss://processed@frml1datalakecourse.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.format("delta").load("abfss://processed@frml1datalakecourse.dfs.core.windows.net/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")