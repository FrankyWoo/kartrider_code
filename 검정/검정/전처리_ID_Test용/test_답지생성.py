# Databricks notebook source
df2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))

df2.registerTempTable("df2")

# COMMAND ----------

df2.count()

# COMMAND ----------

df=spark.sql("select * from df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.registerTempTable("df")

# COMMAND ----------

# df3 = df3.withColumn('name_dayofweek', when((df3.startTime_dayofweek == 2), lit("Mon"))
#                      .when((df3.startTime_dayofweek == 3), lit("Tue"))\
#                      .when((df3.startTime_dayofweek == 4), lit("Wed"))\
#                      .when((df3.startTime_dayofweek == 5), lit("Thu"))\
#                      .when((df3.startTime_dayofweek == 6), lit("Fri"))\
#                      .when((df3.startTime_dayofweek == 7), lit("Sat"))\
#                      .otherwise("Sun")

# COMMAND ----------

df=spark.sql("select * from df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")

sample_pd=df.toPandas()

sample_brody=sample_pd[sample_pd["track"]=="브로디 비밀의 연구소"]
sample_brody["label"] = [0 if  181>= x >=122 else 1 for x in sample_brody["p_matchTime"]]

sample_china_rush=sample_pd[sample_pd["track"]=="차이나 골목길 대질주"]
sample_china_rush["label"] = [0 if  182>= x >=123 else 1 for x in sample_china_rush["p_matchTime"]]

sample_china_dragon=sample_pd[sample_pd["track"]=="차이나 용의 운하"]
sample_china_dragon["label"] = [0 if  179>= x >=120 else 1 for x in sample_china_dragon["p_matchTime"]]

sample_villeage=sample_pd[sample_pd["track"]=="빌리지 운명의 다리"]
sample_villeage["label"] = [0 if  176>= x >=117 else 1 for x in sample_villeage["p_matchTime"]]

sample_basement=sample_pd[sample_pd["track"]=="대저택 은밀한 지하실"]
sample_basement["label"] = [0 if  176>= x >=117 else 1 for x in sample_basement["p_matchTime"]]


# COMMAND ----------

sample_pd

# COMMAND ----------

import pandas as pd
concated=pd.concat([sample_brody,sample_china_rush,sample_china_dragon,sample_villeage,sample_basement])

# COMMAND ----------

concated

# COMMAND ----------

concated['label'].value_counts()

# COMMAND ----------

sparkDF=spark.createDataFrame(concated) 

# COMMAND ----------

sparkDF.display()

# COMMAND ----------

sparkDF.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versioA_labeld_test.csv")

# COMMAND ----------

###cooksdistance 답지 
df2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versioA_cdTest.csv"))

df2.registerTempTable("df2")

# COMMAND ----------

df3=spark.sql("select * from df2 where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
df3.display()

# COMMAND ----------

sample_pd=df3.toPandas()

sample_brody=sample_pd[sample_pd["track"]=="브로디 비밀의 연구소"]
sample_brody["label"] = [0 if  181>= x >=122 else 1 for x in sample_brody["p_matchTime"]]

sample_china_rush=sample_pd[sample_pd["track"]=="차이나 골목길 대질주"]
sample_china_rush["label"] = [0 if  182>= x >=123 else 1 for x in sample_china_rush["p_matchTime"]]

sample_china_dragon=sample_pd[sample_pd["track"]=="차이나 용의 운하"]
sample_china_dragon["label"] = [0 if  179>= x >=120 else 1 for x in sample_china_dragon["p_matchTime"]]

sample_villeage=sample_pd[sample_pd["track"]=="빌리지 운명의 다리"]
sample_villeage["label"] = [0 if  176>= x >=117 else 1 for x in sample_villeage["p_matchTime"]]

sample_basement=sample_pd[sample_pd["track"]=="대저택 은밀한 지하실"]
sample_basement["label"] = [0 if  176>= x >=117 else 1 for x in sample_basement["p_matchTime"]]


# COMMAND ----------

import pandas as pd
concated=pd.concat([sample_brody,sample_china_rush,sample_china_dragon,sample_villeage,sample_basement])

# COMMAND ----------

concated

# COMMAND ----------

concated