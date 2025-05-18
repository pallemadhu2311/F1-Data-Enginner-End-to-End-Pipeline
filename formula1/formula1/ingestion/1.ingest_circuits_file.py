# Databricks notebook source
# MAGIC %run "../includes/configuration" 
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/commonfunctions"

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####1. Ingest the Circuits CSV file
# MAGIC

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.frml1datalakecourse.dfs.core.windows.net",
    "ZWIl6EPz74qlrHWfPI+4fNeNNJwg8NYGcAcmENrEPTBO7125wMDW0wRtN+ZvlIsDgxGvSuSdjWUb+AStOli28w=="
)

# COMMAND ----------


display(dbutils.fs.ls("abfss://raw@frml1datalakecourse.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_data}/circuits.csv")


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####2.  Select only the Required Columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
display(circuits_selected_df)


# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

display(circuits_selected_df)


# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

display(circuits_selected_df)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 3. Rename the Columns as Required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed_df = circuits_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_data))

# COMMAND ----------

display(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 5. Add Ingestion date to the dataframe

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

#circuits_final_df = circuit_renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

circuits_final_df = ingest_current_timestamp(circuit_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df = circuit_renamed_df.withColumn("ingestion_date",current_timestamp()) \
    .withColumn("env",lit("Production"))
    
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 6. Write data to  datalake as Parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")



# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@frml1datalakecourse.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")