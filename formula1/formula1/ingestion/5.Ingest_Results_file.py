# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest the Results.json file 

# COMMAND ----------

spark.read.json(f"{raw_folder_path}/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT raceId, count(1)
# MAGIC FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json(f"{raw_folder_path}/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT raceId, count(1)
# MAGIC FROM results_w1
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json(f"{raw_folder_path}/2021-04-18/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT raceId, count(1)
# MAGIC FROM results_w1
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

dbutils.widgets.text("p_data_source", "test")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 :  Read the Results.json file using the spark dataframe Reader API

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("fastestLap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", FloatType(), True),
                                   StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 : Rename the Columns and Add new Columns (ingestion date)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

results_with_column_df = results_df.withColumnRenamed("resultId","result_id") \
                                .withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("constructorId","constructor_id") \
                                .withColumnRenamed("positionText","position_text") \
                                .withColumnRenamed("positionOrder","position_order") \
                                .withColumnRenamed("fastestLap","fastest_lap") \
                                .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(results_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 :  Drop the unwanted columns with col

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_column_df.drop(col("statusId"))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### removing Duplicated Data
# MAGIC

# COMMAND ----------


results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4 : Write the output to Processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method - 1 (Incremental Loading)

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
     column_list = []
     for column_name in input_df.schema.names:
         if column_name != partition_column:
             column_list.append(column_name)
     column_list.append(partition_column)
     output_df = input_df.select(column_list)
     return output_df

output_df = re_arrange_partition_column(results_final_df, "race_id")
display(output_df)

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results drop IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# %sql
# SELECT * FROM f1_processed.results;

# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

merge_delta_data(results_deduped_df,'f1_processed','results', processed_folder_path , merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC WHERE file_date='2021-03-21';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC
# MAGIC GROUP BY race_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC order by race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id,driver_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id,driver_id
# MAGIC HAVING count(1)>1
# MAGIC ORDER BY race_id,driver_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.results WHERE race_id=540 AND driver_id=229;

# COMMAND ----------

dbutils.notebook.exit("Success")