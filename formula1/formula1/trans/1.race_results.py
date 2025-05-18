# Databricks notebook source
# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
                .withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f"{processed_folder_path}/circuits") \
                .withColumnRenamed("location", "circuit_location")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
                .withColumnRenamed("name", "race_name") \
                .withColumnRenamed("race_timestamp", "race_date") \
                .withColumnRenamed("circuit_Id","circuit_id") \
                .withColumnRenamed("race_Id", "race_id")

display(races_df)

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}/results") \
        .filter(f"file_date = '{v_file_date}'") \
        .withColumnRenamed("time", "race_time") \
        .withColumnRenamed("race_id","result_race_id") \
        .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Join Circuits to Races
# MAGIC

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner").select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner").join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner").join(constructor_df, results_df.constructor_id == constructor_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","result_file_date")\
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df)

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

output_df = re_arrange_partition_column(final_df, "race_id")
display(output_df)

# COMMAND ----------

#overwrite_partition(output_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name  AND tgt.race_id = src.race_id"

merge_delta_data(output_df,'f1_presentation','race_results', presentation_folder_path , merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC order by race_id DESC;