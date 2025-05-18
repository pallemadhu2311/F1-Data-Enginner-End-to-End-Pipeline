# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id<70") \
    .withColumnRenamed("name","circuit_name")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name","race_name")
display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"inner")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"inner").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Outer Join

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"left").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"right").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# full Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"full").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### semi joins

# COMMAND ----------

# Semi Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"semi").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Semi Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Anti Joins

# COMMAND ----------

# Anti Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_Id,"anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Anti Join
race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_Id,"anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Cross Joins

# COMMAND ----------

race_circuits_df =races_df.crossJoin(circuits_df)
display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

int(races_df.count() * circuits_df.count())

# COMMAND ----------

