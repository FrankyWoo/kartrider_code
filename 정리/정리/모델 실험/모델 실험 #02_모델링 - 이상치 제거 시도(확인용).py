# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august7.csv"))

august.registerTempTable("august")

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september6.csv"))

september.registerTempTable("september")

# df = august.union(september)
# df.registerTempTable("df")

# COMMAND ----------

aa = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/correlation.csv"))

# COMMAND ----------

aa.display()

# COMMAND ----------

df = august.union(september)
df.registerTempTable("df")

# COMMAND ----------

df.columns

# COMMAND ----------

df.select('p_matchTime').summary().show()

# COMMAND ----------

df.select('playTime').summary().show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select channelName , count(channelName) as cnt  from df group by channelName

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(channelName) as cnt from df where channelName = 'grandprix_speedTeamInfinit' group by track

# COMMAND ----------

# MAGIC %md 
# MAGIC 특정 매치들에 대해 그랑프리전 데이터가 몰려있습니다. 따라서, 그랑프리 채널이 track에 따라 의미가 있을 거라 판단해 제거하지 않았습니다. </br>
# MAGIC 8월과 9월에 대한 그랑프리전  정보를 수집해 그랑프리 매치를 따로 구분하고 싶었지만, 자료를 찾지 못했습니다.  </br>
# MAGIC 그러나 위 표만 참고하여도 그랑프리전에 의미 있을 데이터를 구분해낼 수 있지 않나 생각됩니다.  </br>
# MAGIC track이 모두 원핫인코딩 된다는 측면에서 feature를 추가적으로 만들 필요가 없지 않나 판단됩니다. </br>
# MAGIC 그러나 회귀 모델 특정 상 feature가 많을 수록 R^2 값이 증가할 것이라 판단되어 ...................... 첫 모델 정확도가 낮을 경우 이후에 feature 추가... 하겠습니당

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md 
# MAGIC track별로 데이터 개수 확인 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(track) as cnt from df group by track

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(track) as cnt from df group by track having count(track) >= 1000

# COMMAND ----------

tmp = spark.sql("select track, count(track) as cnt from df group by track having count(track) >= 1000")
tmp.registerTempTable("tmp")

# COMMAND ----------

df3 = spark.sql("""
select a.*, b.cnt
from df as a
left join tmp as b
on 1=1
and a.track = b.track
""")

df3.registerTempTable("df3")

# COMMAND ----------

df3.display()

# COMMAND ----------

from pyspark.sql import functions as F
df4 = df3.where(F.col('cnt').isNotNull())
df4.display()

# COMMAND ----------

print(df3.count())
print(df4.count())

# COMMAND ----------

df4.registerTempTable("df4")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ----------------------------------------이상치 제거--------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md 
# MAGIC 01. 각 track에 대해 0.01 (백분위 수 1과 99) 기준. 
# MAGIC 02. ....0.001...?

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### 각 track에 대해 0.01, 0.99 

# COMMAND ----------

df4.display()

# COMMAND ----------


df5 = spark.sql("select track,  percentile_approx(p_matchTime,0.99) as upper_bnd, percentile_approx(p_matchTime,0.01) as lower_bnd from df4 group by track")
df5.registerTempTable("df5")
df5.display()

# COMMAND ----------

df6 = spark.sql("""
select a.*, b.upper_bnd, b.lower_bnd
from df4 as a
left join df5 as b
on 1=1
and a.track = b.track
""")

df6.registerTempTable("df6")

# COMMAND ----------

df6.display()

# COMMAND ----------

df7 = spark.sql("select * from df6 where p_matchTime between lower_bnd and upper_bnd")
df7.display()

# COMMAND ----------

print(df6.count())
print(df7.count())

# COMMAND ----------

#df7.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/upperlower.csv")

# COMMAND ----------

df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/upperlower.csv"))

df.registerTempTable("df")

# COMMAND ----------

# MAGIC %md
# MAGIC track 별로 playTime과 matchTime 관계

# COMMAND ----------

import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.fuancel

Running command...
(1) Spark Jobs
playTime과 matchTime 관계 track별 비교

nctions import PandasUDFType
from pyspark.sql.types import DoubleType
@pandas_udf(DoubleType(), functionType=PandasUDFType.GROUPED_AGG)
def f(x, y):
    return np.minimum(x, y).corr()

# COMMAND ----------

df.display()

# COMMAND ----------

type(df.groupBy('track'))


# COMMAND ----------

### Track별로 playTime과 matchTime의 관계 비교 
#SELECT (Avg(x * y) – (Avg(x) * Avg(y))) / (StDevP(x) * StDevP(y)) AS ‘Pearsons r’

# COMMAND ----------

tmp = spark.sql("select track, count(track) as cnt, collect_list(playTime) as playtime, collect_list(p_matchTime) as matchtime from df group by track")
tmp.display()

# COMMAND ----------

plz = spark.sql("select track, collect_list(playTime) as playtime, collect_list(p_matchTime) as matchtime from df group by track")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(track) from df group by track order by 2 desc

# COMMAND ----------

df.registerTempTable("df")

# COMMAND ----------

tmp.registerTempTable("tmp")

# COMMAND ----------

###track to list
track_list = tmp.select("track").rdd.flatMap(lambda x: x).collect()
len(track_list)

# COMMAND ----------

column_1 = df["a"]
column_2 = df["c"]
correlation = column_1.corr(column_2)

# COMMAND ----------

import pandas as pd
result = []
for x in track_list:
#	220000 list 2
  query = "select playtime, matchtime from tmp where track ='{}'".format(x)
#sqlContext.sql(query)
 # temp = sqlContext.sql(s"select playtime, matchtime from tmp where track = $x")
  #temp = sql(where track = tract group tarck)
  temp = spark.sql(query)
  temp2 = temp.toPandas()
  cor = temp2[['playtime', 'matchtime']].corr()
  #cor = temp2['playtime'].corr(temp2['matchtime'])	
  result.append(cor)
# table['Group'].corr(table['Age'])

# COMMAND ----------

len(result)

# COMMAND ----------

correlation = pd.DataFrame(list(zip(track_list, result)),
              columns=['trackname','corr'])

# COMMAND ----------

result

# COMMAND ----------

track_list

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tmp

# COMMAND ----------

aa = spark.sql("select playtime, matchtime from tmp where track = '노르테유 붕붕 점프'")
aa = aa.toPandas()

# COMMAND ----------

aa

# COMMAND ----------

aa['playtime'][0]

# COMMAND ----------

import numpy as np
from scipy import stats 
np.correlate(aa['playtime'][0],aa['matchtime'][0])
r,p= stats.mstats.pearsonr(aa['playtime'][0],aa['matchtime'][0])

# COMMAND ----------

r

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy import stats 
r_value = []
p_value = []
for x in track_list:
  query = "select playtime, matchtime from tmp where track ='{}'".format(x)
  temp = spark.sql(query)
  temp2 = temp.toPandas()
  r,p= stats.mstats.pearsonr(temp2['playtime'][0],temp2['matchtime'][0])
  r_value.append(r)
  p_value.append(p)

# COMMAND ----------

correlation = pd.DataFrame(list(zip(track_list, r_value, p_value)),
              columns=['track_list','r_value','p_value'])

# COMMAND ----------

correlation

# COMMAND ----------

r_value

# COMMAND ----------

p_value

# COMMAND ----------

/dbfs/FileStore/KartRider

# COMMAND ----------

correlation.to_csv("/dbfs/FileStore/KartRider/correlation.csv")

# COMMAND ----------

### 이상치 범위 지정. 
### 백분위수를 1과 99로 설정 => IQR 기준과 동일한 잠재적 이상값 제공
### Quantile Rank in pyspark
from pyspark.sql.window import Window
import pyspark.sql.functions as F
# sqlContext.sql("select agent_id, percentile_approx(payment_amount,0.95) as approxQuantile from df group by agent_id")
# df_basket1 = df_basket1.select("Item_group","Item_name","Price", F.ntile(4).over(Window.partitionBy().orderBy(df_basket1['price'])).alias("quantile_rank"))
# df_basket1.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select percentile_approx(p_matchTime,0.99) from df 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select percentile_approx(p_matchTime,0.01) from df 

# COMMAND ----------

df2 = spark.sql("select * from df where p_matchTime < 197.357 and p_matchTime > 62.196")
df2.display()

# COMMAND ----------

df2

# COMMAND ----------

### 다시!! 어떻게 분류할 지 고민. 
#columns_to_drop = ['trackId']
columns_to_drop = ['retired_rate', 'playTime']
df = df.drop(*columns_to_drop)

# COMMAND ----------

### 2021 8월, 9월 해당 그랑프리 맵이 보이지 않습니다... 사실 수치 중심으로 분류해도 괜찮을 것 같은데..... 고민입니다ㅠ
df.columns

# COMMAND ----------

aa.display() #### 그림. retired 비율, 난이도 같이 비교해서 density 그래프 그리기. 