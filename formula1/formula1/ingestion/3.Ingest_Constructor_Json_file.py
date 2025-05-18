# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Ingest Constructor JSON File 

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
# MAGIC ##### Step 1 : Read the JSON file using the spark dataframe reader

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/{v_file_data}/constructors.json")
constructor_df.write.mode("overwrite")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 : Drop the unwanted columns {URL}

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Method - 1

constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

constructor_dropped_df

# COMMAND ----------

# Method - 2
## constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 : Rename the columns and Add the Ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_data))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 4 : Write the output to the Parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").save("abfss://processed@frml1datalakecourse.dfs.core.windows.net/constructors")

# COMMAND ----------

constructor_final_df.write.mode("overwrite").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------


display(dbutils.fs.ls("abfss://processed@frml1datalakecourse.dfs.core.windows.net/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")