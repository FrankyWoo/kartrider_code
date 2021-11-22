# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september5.csv"))

september.registerTempTable("september")


df = august.union(september)
df.registerTempTable("df")

# COMMAND ----------

ratio = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/retiredratiotable.csv"))

ratio.registerTempTable("ratio")

# COMMAND ----------

df.display()

# COMMAND ----------

ratio.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select retiredRatio from ratio where track = '브로디 비밀의 연구소'

# COMMAND ----------

final = spark.sql("""
               select a.*, b.retiredRatio 
               from df as a 
               left join ratio as b 
               on 1=1
               and a.track = b.track""")
final.registerTempTable("final")

# COMMAND ----------

final.display()

# COMMAND ----------

final.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versionBaug6sep5.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC 두번째

# COMMAND ----------

august2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august7.csv"))

august2.registerTempTable("august2")

september2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september6.csv"))

september2.registerTempTable("september2")


df2 = august2.union(september2)
df2.registerTempTable("df2")

# COMMAND ----------

final2 = spark.sql("""
               select a.*, b.retiredRatio 
               from df2 as a 
               left join ratio as b 
               on 1=1
               and a.track = b.track""")
final2.registerTempTable("final2")

# COMMAND ----------

final2.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select retiredRatio from ratio where track = '차이나 라사'

# COMMAND ----------

columns_to_drop = ['retired_ratio']
final2 = final2.drop(*columns_to_drop)

# COMMAND ----------

final2.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versionBaug7sep6.csv")