# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august7.csv")) ### august7 : 결측지가 전혀 없는 파일

august.registerTempTable("august")

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september6.csv")) ### september6 : 결측치가 전혀 없는 파일

september.registerTempTable("september")

# COMMAND ----------

df = august.union(september)
df.registerTempTable("df")

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august.csv")) ### august7 : 결측지가 전혀 없는 파일

august.registerTempTable("august")

# COMMAND ----------

#df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/gamedataproject.csv")

# COMMAND ----------

august2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv"))

september2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september4.csv"))


august2.registerTempTable("august")
september2.registerTempTable("september")

# COMMAND ----------

df2 = august2.union(september2)
df2.registerTempTable("df2")

# COMMAND ----------

tmp = spark.sql("select track, count(distinct matchid) as match_count from df2 group by track order by 2 desc ")
tmp.display()

# COMMAND ----------

import plotly.express as px
#tmp2 = tmp.toPandas()
fig = px.bar(tmp2, x='track', y='match_count')
fig.show()

# COMMAND ----------

tmp3 = spark.sql("select track, count(distinct matchid) as match_count from df2  group by track having count(distinct matchid) >= 2000 order by 2 ")
tmp3.display()

# COMMAND ----------

#tmp4 = tmp3.toPandas()
fig = px.bar(tmp4, x='track', y='match_count', width=2000, height=1000)
fig.show()

# COMMAND ----------

df2.count()

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv")) ### august7 : 결측지가 전혀 없는 파일

august.registerTempTable("august")

# COMMAND ----------

august.count()

# COMMAND ----------

from plotly.figure_factory import create_distplot
import pandas as pd
import numpy as np
import plotly.express as px
# df = tmp.select("*").toPandas()
# fig = px.histogram(df, x="track", y="matchtime", color="track", marginal="rug")
# fig.show()

# COMMAND ----------

tmp = spark.sql("select track, count(distinct matchId) as match_count, collect_list(p_matchTime) as matchtime from august group by track order by 2")
tmp = tmp.filter(tmp.match_count>=1000)

# COMMAND ----------

tmp.registerTempTable("tmp")
spark.sql("select track, match_count from tmp").display()

# COMMAND ----------

august.display()

# COMMAND ----------

from pyspark.sql.functions import when,col
august2 = august.withColumn("weekgroup", when(august.startTime_day <= 7,1)
                                 .when((august.startTime_day > 7) & (august.startTime_day <= 14),2)
                                 .when((august.startTime_day > 14) & (august.startTime_day <= 21),3)
                                 .when((august.startTime_day > 21) & (august.startTime_day <= 28),4)
                                 .otherwise(""))
august2.display()

# COMMAND ----------

august2.registerTempTable("tmp2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_day, weekgroup from tmp2 group by startTime_day, weekgroup

# COMMAND ----------

# MAGIC %sql 
# MAGIC select weekgroup, count(*) as user_count from tmp2 group by weekgroup order by count(*) 

# COMMAND ----------

tmp3 = spark.sql("select * from tmp2 where weekgroup = 3")

# COMMAND ----------

collect_list(p_matchTime) as matchtime

# COMMAND ----------

tmp3.registerTempTable("tmp3")

# COMMAND ----------

#tmp4 = spark.sql("select track, count(distinct matchId) as match_count, collect_list(p_matchTime) as matchtime from tmp3 group by track order by 2")
tmp4 = tmp4.filter(tmp4.match_count>=1478)

# COMMAND ----------

tmp4.registerTempTable("tmp4")

# COMMAND ----------

tmp4.select(size(tmp4.matchtime))

# COMMAND ----------

tmp4.display()

# COMMAND ----------

x0 = tmp4.collect()[0][2]
x1 = tmp4.collect()[1][2]
x2 = tmp4.collect()[2][2]
x3 = tmp4.collect()[3][2]
x4 = tmp4.collect()[4][2]
x5 = tmp4.collect()[5][2]
x6 = tmp4.collect()[6][2]
x7 = tmp4.collect()[7][2]
x8 = tmp4.collect()[8][2]
x9 = tmp4.collect()[9][2]

# COMMAND ----------

### densityy graph
fig = create_distplot(
    hist_data, group_labels,curve_type='normal',
    show_rug=False, bin_size=10)

fig['layout'].update(width=1080)
fig['layout'].update(height=720)
fig.show()

# COMMAND ----------

### densityy graph
fig = create_distplot(
    hist_data, group_labels,curve_type='normal',
    show_rug=False, bin_size=10)

fig['layout'].update(width=1500)
fig['layout'].update(height=720)
fig.show()

# COMMAND ----------

hist_data = [x0, x1, x2, x3, x4, x5, x6, x7, x8, x9]
group_labels = [ tmp.collect()[0][0], tmp.collect()[1][0],tmp.collect()[2][0],tmp.collect()[3][0],tmp.collect()[4][0],tmp.collect()[5][0],tmp.collect()[6][0],tmp.collect()[7][0],tmp.collect()[8][0],tmp.collect()[9][0]]

# COMMAND ----------

import plotly.express as px
#df = px.data.tips()
fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",
                   hover_data=df.columns)
fig.show()