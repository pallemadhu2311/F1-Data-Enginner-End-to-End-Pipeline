# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest Pitstop.json (MultiLine) file 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 : Read the JSON file using the spark dataframe Reader API

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

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC * Spark By Default does not deal with Multi Line Json objects. 
# MAGIC * That is why we are seeing null values

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiline", "true") \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 : Rename Columns and add new Columns
# MAGIC
# MAGIC 1. Rename driverId and raceid
# MAGIC 1. Add ingestion date with current timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))
                                    

# COMMAND ----------

display(pit_stops_final_df)

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

final_df = re_arrange_partition_column(pit_stops_final_df, "race_id")
display(final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### step 3: Write to output to processed container in parquet file
# MAGIC

# COMMAND ----------

#overwrite_partition(output_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id =src.driver_id AND tgt.stop = src.stop AND tgt.lap = src.lap AND tgt.race_id = src.race_id"

merge_delta_data(final_df,'f1_processed','pit_stops', processed_folder_path , merge_condition, 'race_id')

# COMMAND ----------

display(spark.read.format("delta").load("abfss://processed@frml1datalakecourse.dfs.core.windows.net/pit_stops"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_processed.pit_stops

# COMMAND ----------

dbutils.notebook.exit("Success")