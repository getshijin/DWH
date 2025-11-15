# Databricks notebook source
# MAGIC %sql
# MAGIC select * from read_files("/Volumes/demo/schema_evo/kagggle_market_price",
# MAGIC format=>"csv")
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo;
# MAGIC use schema schema_evo;

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
# MAGIC --preview 
# MAGIC select * from market_data_ctas_bronze
# MAGIC limit 10; 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended market_data_ctas_bronze

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("/Volumes/demo/schema_evo/kagggle_market_price/2001.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table market_data_ctas_bronze_ci(state string,District string);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into market_data_ctas_bronze_ci
# MAGIC from '/Volumes/demo/schema_evo/kagggle_market_price'
# MAGIC FILEFORMAT = CSV
# MAGIC copy_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

