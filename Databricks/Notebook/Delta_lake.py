# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_managed (
# MAGIC order_id BIGINT,
# MAGIC sku STRING,
# MAGIC product_name STRING,
# MAGIC product_category STRING,
# MAGIC qty INT,
# MAGIC unit_price DECIMAL(10,2)
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_managed VALUES
# MAGIC   (1,  'A101', 'iPhone Charger',       'Electronics', 2, 19.99),
# MAGIC   (2,  'A102', 'Bluetooth Headphones', 'Electronics', 1, 49.50),
# MAGIC   (3,  'A103', 'HDMI Cable',           'Electronics', 3, 5.99);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED orders_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists workspace.default.dataset1

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/default/dataset1/ordersdata1")

# COMMAND ----------

# MAGIC %fs mkdirs /Volumes/workspace/default/dataset1/ordersdata2

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/dataset1

# COMMAND ----------

# Orders sample data
data = [
    (1, "SKU-1001", "Wireless Mouse", "Electronics", 2, 799.00),
    (2, "SKU-2001", "Yoga Mat", "Fitness", 1, 1199.00),
    (3, "SKU-3001", "Notebook A5", "Stationery", 5, 49.50),
    (4, "SKU-4001", "Coffee Mug", "Kitchen", 3, 299.00),
    (5, "SKU-5001", "LED Bulb", "Electronics", 4, 149.99)
]

# Define schema (column names)
columns = ["order_id", "sku", "product_name", "product_category", "qty", "unit_price"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Display DataFrame
display(df)

# COMMAND ----------

volume_path = "/Volumes/workspace/default/dataset1/ordersdata1"
df.write.format("delta").mode("overwrite").save(volume_path)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("default.ordersdata1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.ordersdata1

# COMMAND ----------

# 3) Incoming batch to upsert
incoming = spark.createDataFrame(
    [(1,"SMS","SMS","Electronics",2,799.00),   # UPDATE id=1
     (6,"SKU-6001","666","666666",4,149.99)],  # INSERT id=6
    ["order_id","sku","product_name","product_category","qty","unit_price"]
)

# 4) Upsert by order_id
tgt = DeltaTable.forName(spark, "default.ordersdata1")
(tgt.alias("t")
 .merge(incoming.alias("s"), "t.order_id = s.order_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

# 5) Verify
spark.table("default.ordersdata1").orderBy("order_id").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted default.ordersdata1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.ordersdata1

# COMMAND ----------

