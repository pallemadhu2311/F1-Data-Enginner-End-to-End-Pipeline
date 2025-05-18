# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest Drivers.Json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 1 : Read the JSON file using the spark dataframe reader API

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

from pyspark.sql.types import *


# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),True),
                                 StructField("surname", StringType(),True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)
                                      ])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_data}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2: Rename columns and add new Columns
# MAGIC
# MAGIC 1. driverId rename to driver_id
# MAGIC 1. driverRef rename to driver_ref
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname 

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_data))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 : Drop the unwanted columns
# MAGIC
# MAGIC 1. name.forename
# MAGIC 1. name.surname
# MAGIC 1. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4 : Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.fs.ls("abfss://processed@frml1datalakecourse.dfs.core.windows.net/drivers")

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@frml1datalakecourse.dfs.core.windows.net/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")