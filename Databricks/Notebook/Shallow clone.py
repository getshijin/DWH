# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog demo;
# MAGIC use schema schema_evo;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists order_managed ; 
# MAGIC
# MAGIC create or replace table order_managed(
# MAGIC   order_id bigint,
# MAGIC   sku string,
# MAGIC   product_name string,
# MAGIC   qty int,
# MAGIC   price double,
# MAGIC   product_category string
# MAGIC );
# MAGIC
# MAGIC alter table order_managed add constraint valid_qty check (qty >0);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into order_managed values 
# MAGIC (101, 'SKU-001', 'Wireless Mouse', 2, 19.99, 'Electronics'),
# MAGIC   (102, 'SKU-002', 'Bluetooth Keyboard', 1, 34.50, 'Electronics'),
# MAGIC   (103, 'SKU-003', 'USB-C Cable', 3, 9.99, 'Accessories'),
# MAGIC   (104, 'SKU-004', 'Notebook', 5, 2.49, 'Stationery'),
# MAGIC   (105, 'SKU-005', 'Water Bottle', 1, 12.00, 'Lifestyle');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history order_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO order_managed VALUES
# MAGIC   (106, 'SKU-006', 'Laptop Stand', 1, 29.99, 'Accessories'),
# MAGIC   (107, 'SKU-007', 'Desk Lamp', 2, 15.75, 'Home'),
# MAGIC   (108, 'SKU-008', 'Running Shoes', 1, 49.90, 'Sports');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from order_managed where order_id = 104

# COMMAND ----------

# MAGIC %sql
# MAGIC update order_managed
# MAGIC set qty =100,
# MAGIC price =222.288
# MAGIC where order_id = 103

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO order_managed VALUES
# MAGIC   (109, 'SKU-009', 'Wireless Earbuds', 1, 39.99, 'Electronics'),
# MAGIC   (110, 'SKU-010', 'Coffee Mug', 2, 5.99, 'Kitchen');
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history order_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table order_manged_shallow_clone shallow clone order_managed;
# MAGIC describe detail order_manged_shallow_clone;

# COMMAND ----------

