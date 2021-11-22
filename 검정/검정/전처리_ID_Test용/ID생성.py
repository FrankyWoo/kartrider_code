# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))

df.registerTempTable("df")

# COMMAND ----------

from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

df2 = df.withColumn('id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.registerTempTable("df2")
df2.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select id, count(distinct id) as cnt from df2 group by id having count(distinct id) != 1

# COMMAND ----------

df2.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versionA_id.csv")