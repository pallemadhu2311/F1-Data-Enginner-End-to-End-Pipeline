# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year") \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list =[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
    
print(race_results_list)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

driver_standing_df = race_results_df \
                        .groupBy("race_year","driver_name","driver_nationality") \
                        .agg(sum("points").alias("total_points"),count(when(col("grid") ==1, True)) \
                        .alias("wins"))

# COMMAND ----------

display(driver_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))

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

output_df.write.format("delta").mode("overwrite").save(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

display(output_df)

# COMMAND ----------

from pyspark.sql.functions import col

output_df = output_df.withColumn("race_year", col("race_year").cast("int"))


# COMMAND ----------

output_df.printSchema()
# Should say: race_year: integer (nullable = true)


# COMMAND ----------

# dbutils.fs.rm(f"{presentation_folder_path}/driver_standings", True)


# COMMAND ----------

# overwrite_partition(output_df, 'f1_presentation', 'driver_standings', 'race_year')


# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"

merge_delta_data(output_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

display(output_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings

# COMMAND ----------

