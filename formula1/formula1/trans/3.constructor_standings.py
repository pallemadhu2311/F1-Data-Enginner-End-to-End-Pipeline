# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, "race_year")

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
                        .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

constructor_standing_df = race_results_df \
                        .groupBy("race_year","team") \
                        .agg(sum("points").alias("total_points"),count(when(col("grid") ==1, True)) \
                        .alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standing_df.withColumn("rank",rank().over(constructor_rank_spec))

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

output_df = re_arrange_partition_column(final_df, "race_year")
display(output_df)

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"

merge_delta_data(output_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from f1_presentation.constructor_standings;