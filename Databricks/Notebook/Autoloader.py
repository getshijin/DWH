# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog auto_load;
# MAGIC create schema schema_evo;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo;
# MAGIC use schema schema_evo;

# COMMAND ----------

# MAGIC %sql
# MAGIC list "/Volumes/demo/schema_evo/kagggle_market_price"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Drop if already exist
# MAGIC drop table if exists market_data_ctas_bronze;
# MAGIC --create table
# MAGIC create table market_data_ctas_bronze
# MAGIC using delta
# MAGIC partitioned by (Arrival_Date)
# MAGIC as
# MAGIC select * from read_files("/Volumes/demo/schema_evo/kagggle_market_price",
# MAGIC format=>"csv");

# COMMAND ----------

# MAGIC %sql
# MAGIC --Drop if already exist
# MAGIC drop table if exists market_data_ctas_bronze;
# MAGIC --create table
# MAGIC create table market_data_ctas_bronze
# MAGIC using delta
# MAGIC partitioned by (Arrival_Date)
# MAGIC as
# MAGIC select *,
# MAGIC _metadata.file_name as file_name,
# MAGIC _metadata.file_path as file_path,
# MAGIC _metadata.file_size as file_size,
# MAGIC _metadata.file_modification_time as modification_time,
# MAGIC current_timestamp() as ingestion_ts
# MAGIC from read_files("/Volumes/demo/schema_evo/kagggle_market_price",
# MAGIC format=>"csv");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from market_data_ctas_bronze limit 10;