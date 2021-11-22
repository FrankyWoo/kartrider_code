# Databricks notebook source
#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/normaldata1.csv"))

final.registerTempTable("finalsql")

# COMMAND ----------

finaldf = final.select("*").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### normal data는 정제된 데이터 맞는 지 확인. 

# COMMAND ----------

spark.sql("select * from finalsql where playTime < 10").display()

# COMMAND ----------

 spark.sql("select * from finalsql where (channelName like '%Newbie') and (p_rankinggrade2 >3) ").display()

# COMMAND ----------

finaldf['channelName'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC #### actual distance 변수 추가 : 거리 X 바퀴 (distance * lap)

# COMMAND ----------

final.display()

# COMMAND ----------

finaldf.info()

# COMMAND ----------

finaldf['lap'].value_counts()

# COMMAND ----------

finaldf['lap'].value_counts()

# COMMAND ----------

finaldf['lap'][0][:1]

# COMMAND ----------

import pandas as pd
#finaldf['lap'] = finaldf['lap'].astype(int)
finaldf['lap'] = pd.to_numeric(finaldf['lap'])

# COMMAND ----------

finaldf['actual_distance'] = ""
finaldf['actual_distance'] = finaldf['lap'] * finaldf['distance']

# COMMAND ----------

finaldf.head(20)

# COMMAND ----------

del finaldf['_c0'] 

# COMMAND ----------

finaldf.to_csv('/dbfs/FileStore/KartRider/normaldata2.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 저장한 데이터 불러오기 

# COMMAND ----------

#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/normaldata2.csv"))

final.registerTempTable("finalsql")
finaldf = final.select("*").toPandas()

# COMMAND ----------

spark.sql("select * from finalsql").display()

# COMMAND ----------

spark.sql("select distinct matchid from finalsql limit 3").display()

# COMMAND ----------

spark.sql("select track, count(*) from finalsql group by track").display()

# COMMAND ----------

tmp3 = spark.sql("select track, matchid, count(*) from finalsql group by track, matchid order by track")
tmp3.display()

# COMMAND ----------

spark.sql("select track, matchid, count(*) from finalsql group by track, matchid order by track desc limit 60").display()

# COMMAND ----------

spark.sql("select track, count(distinct matchid) as match from finalsql group by track order by track desc limit 10").display()

# COMMAND ----------

tmp4 = spark.sql("select track, count(distinct matchid) as match from finalsql group by track order by count(distinct matchid) desc")
tmp4.display()

# COMMAND ----------

tmp5 = tmp4.select("*").toPandas()

# COMMAND ----------

### track 별 확인 #지도 총개수 : 267
tmp = finaldf['track'].value_counts().to_frame()
tmp

# COMMAND ----------

# MAGIC %md
# MAGIC 모든 지도를 전부 할 필요가 있을까?

# COMMAND ----------

tmp.reset_index(level=0, inplace=True)

# COMMAND ----------

tmp.rename(columns={"index": "name", "track": "cnt"}, inplace = True)

# COMMAND ----------

import seaborn as sns
sns.__version__

# COMMAND ----------

pip install seaborn --upgrade

# COMMAND ----------

# MAGIC %md
# MAGIC trackId별로 playTime 분포

# COMMAND ----------

##### trackId별로 cnt 분포

import plotly.express as px

fig = px.bar(tmp, x='name', y='cnt')
fig.show() #=>matchid별로! 

# COMMAND ----------

import plotly.express as px

fig = px.box(tmp, y="cnt")
fig.show()

# COMMAND ----------

tmp.describe()

# COMMAND ----------

import plotly.express as px

fig = px.bar(tmp5, x='track', y='match')
fig.show() #=>matchid별로! 

# COMMAND ----------

### 지도와 거리 로지스틱 회귀 함수 

# COMMAND ----------

final.schema.names

# COMMAND ----------

### lap 변수 : String to int
final.withColumn("salary", col("lap").cast("int"))

# COMMAND ----------

# MAGIC %md 
# MAGIC actualdistance 별로 playTime 분포

# COMMAND ----------

tmp2 = spark.sql('select actual_distance, avg(playTime) from finalsql')
tmp2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC matchTime vs playTime
# MAGIC * matchTime : 선수 개인의 시간 (retired => null)
# MAGIC * playTime : 게임 경기 시간

# COMMAND ----------

spark.sql("select matchId, p_matchTime, p_matchWin, teamId, playTime, p_matchRetired, teamPlayers from finalsql order by matchId").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 사람 수에 따른 playTime 분포 확인

# COMMAND ----------

spark.sql("select teamPlayers, count(*) from finalsql group by teamPlayers").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, count(*) from finalsql group by p_rankinggrade2").display()

# COMMAND ----------

# 한 사람당 평균 playTime #null 값이 황금문명 황금신전을 찾아서" 일 것 같음..... 어떻게 생각하나용
# 그리고...... sql로 quantile
spark.sql("select teamPlayers, track, channelName,p_rankinggrade2, avg(playTime)  from finalsql group by teamPlayers, track, channelName,p_rankinggrade2 order by teamPlayers, track, p_rankinggrade2").display()

# COMMAND ----------

# 한 사람당 평균 playTime #null 값이 황금문명 황금신전을 찾아서" 일 것 같음..... 어떻게 생각하나용
# 그리고...... sql로 quantile
spark.sql("select teamPlayers, track, channelName,p_rankinggrade2, collect_list(playTime) from finalsql group by teamPlayers, track, channelName,p_rankinggrade2 order by teamPlayers, track, p_rankinggrade2 limit 10").display()

# COMMAND ----------

test = spark.sql("select track, channelName, teamPlayers, p_rankinggrade2, (sum(p_matchWin)/count(*)) as winrate, (sum(p_matchRetired)/count(*)) as retiredrate, collect_list(playTime) as playtime from finalsql group by track, channelName, teamPlayers, p_rankinggrade2 order by track, channelName, teamPlayers, p_rankinggrade2")
test.display()

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

slen = udf(lambda s: len(s), IntegerType())

df2 = test.withColumn("cnt", slen(test.playtime))
df2.show()

# COMMAND ----------

df2.describe(['cnt']).show()

# COMMAND ----------

df2.summary().show()

# COMMAND ----------

df2.registerTempTable("df2sql")

# COMMAND ----------

spark.sql("select * from df2sql where cnt == '174742'").display()

# COMMAND ----------

 percentile_approx(payment_amount,0.95) as approxQuantile 

# COMMAND ----------


slen = udf(lambda s: len(s), IntegerType())

df2 = test.withColumn("cnt", slen(test.playtime))
df2.show()

# COMMAND ----------

quantiles = df.approxQuantile("age", [0.25, 0.5, 0.75], 0)

# COMMAND ----------

#128GB - 256GB
slen2 = udf(lambda s: s.approxQuantile(s, [0.01], 0.25))
df3 = df2.withColumn("lowerbound", df2.approxQuantile("x", [0.5], 0.25))
df3.show()
df.approxQuantile("x", [0.5], 0.25)

# COMMAND ----------

df2 = df.withColumn("product_cnt", slen(df.products))
df2.show()

# COMMAND ----------

df.approxQuantile("x", [0.5], 0.25)

# COMMAND ----------

spark.sql("select teamPlayers, track, channelName,p_rankinggrade2, avg(playTime), std(playTime) from finalsql group by teamPlayers, track, channelName,p_rankinggrade2 order by teamPlayers, track, p_rankinggrade2").display()

# COMMAND ----------

df_new = df2.select("*").toPandas()

# COMMAND ----------

df_new['lower'] = ""
df_new['upper'] = ""

# COMMAND ----------

import numpy as np
df_new['lower'] = df_new['playtime'].apply(lambda x : np.quantile(x, 0.01))

# COMMAND ----------

df_new['upper'] = df_new['playtime'].apply(lambda x : np.quantile(x, 0.99))

# COMMAND ----------

df_new

# COMMAND ----------

df_new['std'] = df_new['playtime'].apply(lambda x : np.std(x))

# COMMAND ----------

df_new['outliers'] = df_new[''] if 

# COMMAND ----------

df_new.iloc[0].lower

# COMMAND ----------

df_new['outliers'] = ""
def get_outliers(x):
    cnt = 0
    minnn = np.quantile(x, 0.01)
    maxxx = np.quantile(x, 0.99)
    for i in x :
      if i < minnn or i > maxxx:
        cnt += 1
    return cnt
df_new['outliers'] = df_new['playtime'].apply(lambda x : get_outliers(x))

# COMMAND ----------

df_new

# COMMAND ----------

df_new.to_csv('/dbfs/FileStore/KartRider/tmpoutliertest.csv')

# COMMAND ----------

df_new['outliers'].value_counts()

# COMMAND ----------

kkk = df_new['outliers'].value_counts().to_frame()
kkk

# COMMAND ----------

kkk.reset_index(level=0, inplace=True)

# COMMAND ----------

kkk

# COMMAND ----------

kkk.describe()

# COMMAND ----------

import plotly.express as px

fig = px.bar(kkk, x='index', y='outliers')
fig.show() #=>matchid별로! 