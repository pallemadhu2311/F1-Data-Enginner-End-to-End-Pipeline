# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Aggregate Functions Demo

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Built-In Aggregate Functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count,sum,countDistinct

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count('race_name')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points')).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct("race_name")) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count(DISTINCT race_name)','num_of_rces').show()

# COMMAND ----------

display(demo_df.groupBy('driver_name').sum('points').orderBy('sum(points)', ascending=False))

# COMMAND ----------

from pyspark.sql.functions import countDistinct

# COMMAND ----------

display(demo_df.groupBy("driver_name").agg(sum("points"),countDistinct("race_name")))

# COMMAND ----------

display(demo_df.groupBy("driver_name").agg(sum("points").alias("Total Points"),countDistinct("race_name").alias("Number of Races")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupby("race_year","driver_name") \
        .agg(sum("points").alias("Total Points"),countDistinct("race_name").alias("Number of Races"))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("Total Points"))
demo_grouped_df = demo_grouped_df.withColumn("rank",rank().over(driverRankSpec))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

