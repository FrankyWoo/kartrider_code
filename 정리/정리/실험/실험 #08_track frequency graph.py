# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september5.csv"))

september.registerTempTable("september")

# COMMAND ----------

df = august.union(september)

# COMMAND ----------

df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime) , max(startTime) from september

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime) , max(startTime) from df

# COMMAND ----------

# MAGIC %md
# MAGIC The Average Matchtime Density Graph 

# COMMAND ----------

from pyspark.sql.functions import col
df2 = df.where(col('track').isNotNull())
df2.registerTempTable("df2")

# COMMAND ----------

### The average of MatchTime Density Graph 
tmp = spark.sql("select track, collect_list(p_matchTime) as matchtime, size(collect_list(p_matchTime)) as cnt from df2 group by track order by 2")
#tmp = tmp.filter(tmp.cnt>1000)

# COMMAND ----------

tmp.registerTempTable("tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

from plotly.figure_factory import create_distplot
import numpy as np
import plotly.express as px

# COMMAND ----------

tmpdf = tmp.select("*").toPandas()

# COMMAND ----------

tmpdf['track'].isna().sum()

# COMMAND ----------

tmpdf = tmpdf[tmpdf['track'].notna()]

# COMMAND ----------

# MAGIC %md
# MAGIC #### 우선 맵 나누쟈 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(track) from tmp   
# MAGIC --- density 그래프에 적용할 track 개수 : 282개

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, cnt from tmp order by cnt desc

# COMMAND ----------

# MAGIC %md 
# MAGIC 사분위수를 기준으로 나누고 싶지만, 데이터 개수가 너무 차이가 납니다. 이 경우, 데이터 개수가 많은 맵들은.... 또 다시 클러스터가.... 나가버려서 ㅠㅠㅠㅠ 
# MAGIC 융통성있게 지도를 나누었습니다. 

# COMMAND ----------

tmp.summary().show()

# COMMAND ----------

282/4

# COMMAND ----------

# MAGIC %sql
# MAGIC select track, cnt, row_number() over(order by cnt ) as row_number
# MAGIC from tmp 

# COMMAND ----------

# tmp2 = spark.sql("select track, cnt, row_number() over(order by cnt desc ) as row_number from tmp")
# tmp2.registerTempTable("tmp2")

# COMMAND ----------

tmp2 = spark.sql("select * from tmp")
tmp2.registerTempTable("tmp2")

# COMMAND ----------

# MAGIC %md 
# MAGIC Density Graph

# COMMAND ----------

from plotly.figure_factory import create_distplot
import numpy as np
import plotly.express as px

# COMMAND ----------

tmp2.display()

# COMMAND ----------

tmp3 = spark.sql("select * from tmp2 where cnt >= 1514 order by cnt desc") #총 142개
tmp3.registerTempTable("tmp3")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, cnt from tmp3

# COMMAND ----------

## map 개수가 너무 많다 보니.. p_matchTime 개수가 어느 정도 있는 맵만 선별하였습니다!
## p_matchTime frequency가  1514 이상으로 간추렸습니다. 
#spark.sql("select * from tmp3 where track = '빌리지 운하'")

# COMMAND ----------

###to_pandas
tmp4 = tmp3.select("*").toPandas()

# COMMAND ----------

# apprix_1 = apprix_df.iloc[:2,:]
# apprix_2 = apprix_df.iloc[2:,:]
map1 = tmp4.iloc[:1, :]

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tmp3 where track = '아이스 펭귄타운'

# COMMAND ----------

# MAGIC %md
# MAGIC cnt에 대한 그래프 

# COMMAND ----------

###tmp2 : p_matchtime에 대한 total count 그래프 
tmp2 = spark.sql("select * from tmp2 order by cnt")
tmp2df = tmp2.select("*").toPandas()
fig = px.bar(tmp2df, x="track", y="cnt", title='track frequency', height=600)
fig.show()