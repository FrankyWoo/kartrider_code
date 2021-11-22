# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versioA_labeld_test.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.display()

# COMMAND ----------

### id 생성
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

df = df.withColumn('id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)

# COMMAND ----------

df.display()

# COMMAND ----------

df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df order by id desc

# COMMAND ----------

df.select('id').distinct().collect()

# COMMAND ----------

df.groupBy('id').count().orderBy('count').show()

# COMMAND ----------

df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versioA_labeld_id_test.csv")