
# Formula 1 Data Engineering End to End Pipeline

### Project Overview:
Built an end‑to‑end data pipeline to ingest, process, and visualize Formula 1 racing data on Azure, enabling analytics-ready datasets and interactive dashboards.

### Key Responsibilities & Achievements:

##### Architected & Deployed Infrastructure
  - Provisioned Azure Resource Group, Storage Account (ADLS Gen2), and Databricks workspace with auto‑scaling Spark clusters
  - Configured secure mount points using service principals and secret scopes

##### Bronze → Silver → Gold Data Layering
  1. Ingested raw CSV/JSON into Delta tables in f1_raw schema (bronze)
  1. Cleaned, deduped, and enriched data with PySpark notebooks → f1_processed (silver)
  1. Curated presentation tables & SQL views (race_results, driver_standings, etc.) in f1_presentation (gold)

##### Delta Lake & Incremental Loads
  1. Implemented ACID transactions, schema evolution, and partition‑overwrite patterns (overwrite_partition, re_arrange_partition_column)
  1. Reduced full‑pipeline run time to ~13 min via optimized cluster config and partition pruning

##### Orchestration & Monitoring
* Configured Databricks Jobs for scheduled notebook runs; set up success/failure alerts and cluster auto‑termination policies 

##### Databricks SQL Visualizations
* Authored SQL queries and bar/area charts to surface top 10 drivers of all time and year‑over‑year team performance trends

##### Tools Used
**Skills:**
. Azure Databricks 
· ADLS Gen2 
· PySpark 
· Databricks SQL 
· Python (Programming Language) 
· Azure Data Factory 
· Data Engineering 
· Data Pipelines 
· Microsoft Azure 
· Azure Data Bricks 
· Azure DataLake 
· Databricks SQL 
· Azure Data Factory 
· SQL 
· Azure DeltaLake 
