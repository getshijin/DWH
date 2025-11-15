# Databricks notebook source
from pyspark.sql.functions import current_timestamp, current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists orders_ext_01;
# MAGIC
# MAGIC create table orders_ext_01 (
# MAGIC   order_id BIGINT,
# MAGIC   sku STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   qty BIGINT,
# MAGIC   unit_price double)
# MAGIC    USING DELTA;
# MAGIC

# COMMAND ----------

for i in range(100):
    order_id = 1000+ i
    sku = 'SKU'+str(i)
    product_name = 'Product'+str(i)
    product_category = 'Category'+str(i)
    qty = 100+i
    unit_price = 100+i
    sql= f""" Insert into orders_ext_01 values ({order_id},'{sku}','{product_name}','{product_category}',{qty},{unit_price});"""

    spark.sql(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders_ext_01;

# COMMAND ----------

# MAGIC %sql
# MAGIC --volume '/Volumes/workspace/default/dataset1/ordersdata2/'
# MAGIC create volume if not exists my_demo_volume

# COMMAND ----------

data = [(1,'Shijin'),(2,'Anju'),(3,'Ram')]
df = spark.createDataFrame(data, schema=['id','name'])


# COMMAND ----------

display(df)

# COMMAND ----------

volum_path = '/Volumes/workspace/default/my_demo_volume/dv_demo/'
df.write.mode('overwrite').save(volum_path)

# COMMAND ----------

data = [(4,'Shijin'),(5,'Anju'),(6,'Ram')]
df1 = spark.createDataFrame(data, schema=['id','name'])

# COMMAND ----------

df1.write.mode('append').save(volum_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/default/my_demo_volume/dv_demo/` order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.`/Volumes/workspace/default/my_demo_volume/dv_demo/` where id=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/default/my_demo_volume/dv_demo/` order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/Volumes/workspace/default/my_demo_volume/dv_demo/`

# COMMAND ----------

