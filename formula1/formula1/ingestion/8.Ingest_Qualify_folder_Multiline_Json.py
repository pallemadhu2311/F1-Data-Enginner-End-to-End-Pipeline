# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest Qualifying Folder (MultiLine) files

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "test")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 : Read the JSON file using the spark dataframe Reader API

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

Qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True),
                                    ])


# COMMAND ----------

Qualifying_df = spark.read \
    .schema(Qualifying_schema) \
    .option("multiLine",True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(Qualifying_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC * Spark By Default does not deal with Multi Line Json objects. 
# MAGIC * That is why we are seeing null values

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 : Rename Columns and add new Columns
# MAGIC
# MAGIC 1. Rename driverId and raceid
# MAGIC 1. Add ingestion date with current timestamp

# COMMAND ----------

Qualifying_final_df = Qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                                    .withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumnRenamed("constructorId","constructor_id") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))
                                    

# COMMAND ----------

display(Qualifying_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### step 3: Write to output to processed container in parquet file
# MAGIC

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = input_df.columns
    if partition_column in column_list:
        column_list.remove(partition_column)
        column_list.append(partition_column)
        output_df = input_df.select(column_list)
    else:
        output_df = input_df
    return output_df

output_df = re_arrange_partition_column(Qualifying_final_df, "race_id")
display(output_df)

# COMMAND ----------

# overwrite_partition(output_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"

merge_delta_data(output_df,'f1_processed','qualifying', processed_folder_path , merge_condition, 'race_id')

# COMMAND ----------

display(spark.read.format("delta").load("abfss://processed@frml1datalakecourse.dfs.core.windows.net/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")