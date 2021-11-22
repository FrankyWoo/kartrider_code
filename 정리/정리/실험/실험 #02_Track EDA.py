# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv"))

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september4.csv"))


august.registerTempTable("august")
september.registerTempTable("september")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Goal 
# MAGIC 1. trackid 별로 평균, 중앙값 랩타임 계산해서 그래프 보여주기
# MAGIC 2. trackname 별로 평균, 중앙값 매치타임 계산해서 그래프 보여주기 
# MAGIC 3. 선별 track 지정 
# MAGIC 4. 선별된 일부 track에 대하여 trackid denstiy 비교

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from august where channelName like '%Newbie';

# COMMAND ----------

import plotly
plotly.__version__

# COMMAND ----------

from __future__ import print_function #python 3 support
print(sc)

# COMMAND ----------

!pip install the chart-studio

# COMMAND ----------

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

import chart_studio.plotly as py
import plotly.graph_objs as go
import pandas as pd
import requests
requests.packages.urllib3.disable_warnings()

# COMMAND ----------

import plotly.express as px
averagedf = average.select("*").toPandas()
fig = px.line(averagedf, x="trackId", y="matchTimeavg", title='matchTime(avg) by trackId')
fig.show()

# COMMAND ----------

### trackId별로 matchTime 중앙값
import pandas as pd
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext 
sql = SQLContext(sc)
import pyspark.sql.functions as func 

medianvalue = august.groupBy("trackId").agg(func.percentile_approx("p_matchTime", 0.5).alias("median"))
medianvalue = medianvalue.withColumnRenamed("median", "medianvalue")
medainvaluedf = medianvalue.select("*").toPandas()
fig = px.line(medainvaluedf, x="trackId", y="medianvalue", title='matchTime(median) by trackId')
fig.show()

# COMMAND ----------

### trackname별로 matchTime 평균
av2 = spark.sql("select track, avg(p_matchTime) as mean from august group by track order by 2 ")
av2.display()

# COMMAND ----------

av2df = av2.select("*").toPandas()
fig = px.line(av2df, x="track", y="mean", title='matchTime(avg) & track name')
fig.show()

# COMMAND ----------

fig = px.bar(av2df, x="track", y="mean", title='matchTime(avg) & track', height=600)
fig.show()

# COMMAND ----------

import plotly.graph_objects as go

fig = go.Figure([go.Bar(x=av2df['track'], y=av2df['mean'])])
fig.update_layout(
    autosize=False,
    width=3000,
    height=500,
    yaxis=dict(
        title_text="mean",
        tickmode="array",
        titlefont=dict(size=30),
    )
)
fig.show()

# COMMAND ----------

### trackname별로 matchTime median
medianvalue = spark.sql("select track, percentile_approx(p_matchTime, 0.5) as median_ from august group by track order by 2")
medianvalue.display()

# COMMAND ----------

### trackname별로 matchTime median
import plotly.express as px
import plotly.graph_objects as go
# medianvalue = spark.sql("select track, percentile_approx(p_matchTime, 0.5) as median_ from august group by track order by 2")
#medainvaluedf = medianvalue.select("*").toPandas()
fig = px.bar(medainvaluedf, x="track", y="median_", title='matchTime(median) & track', height=600)
fig.show()

# COMMAND ----------

import plotly.graph_objects as go

fig = go.Figure([go.Bar(x=medainvaluedf['track'], y=medainvaluedf['median_'])])
fig.update_layout(
    autosize=False,
    width=3000,
    height=500,
    yaxis=dict(
        title_text="median_",
        tickmode="array",
        titlefont=dict(size=30),
    )
)
fig.show()

# COMMAND ----------

### trackId 별로 matchTime 평균
average = august.groupBy("trackId").agg({'p_matchTime':'avg'})
average = average.withColumnRenamed("avg(p_matchTime)", "matchTimeavg")
average.display()

# COMMAND ----------

### track별로 매치 수 확인 
tmp = spark.sql("select track, count(distinct matchId) as cnt from august group by track order by 2")
tmp.display()

# COMMAND ----------

import plotly.graph_objects as go

tmpdf = tmp.select("*").toPandas()
fig = go.Figure([go.Bar(x=tmpdf['track'], y=tmpdf['cnt'])])
fig.update_layout(
    autosize=False,
    width=3000,
    height=500,
    yaxis=dict(
        title_text="count",
        tickmode="array",
        titlefont=dict(size=30),
    )
)
fig.show()

# COMMAND ----------

fig = px.bar(tmpdf, x="track", y="cnt", title='track frequency', height=600)
fig.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 선별 트랙 지정 과정 

# COMMAND ----------

### 10집단씩 묶기 (meanTime 기준)
av2df 

# COMMAND ----------

df = spark.sql("select track, avg(p_matchTime) as mean from august group by track order by 2")
tmp = spark.sql("select track, count(distinct matchId) as cnt from august group by track order by 2")
#df.filter(df.mean > 10000).show()

# COMMAND ----------

### 우선 1000번 아래 매치 진행 데이터의 경우 제거 
tmp.filter(tmp.cnt > 1000).display()

# COMMAND ----------

df.join(tmp, df.track == tmp.track).select(df["*"],tmp["*"]).display()

# COMMAND ----------

from pyspark.sql.functions import *

m = df.join(tmp, df.track == tmp.track).select(df["*"],tmp["cnt"])
m.display()

# COMMAND ----------

m = m.filter(m.cnt>1000)
m.count()

# COMMAND ----------

m.display()

# COMMAND ----------

#m.registerTempTable("m")
m = spark.sql("select * from m order by 3 desc")

# COMMAND ----------

## cnt 기준으로 10개씩 집단을 나누기로 최종 결정. 
## 이유 : 1000개 이하 자른 것도 cnt 기준이었으며, 평균이 비슷한 track 끼리 비교할 시 density를 그래프로 파악하기 힘듦. cnt 기준일 경우, 선호도가 비슷한 맵 간의 density를 파악하기 용이하다 생각함. 

# COMMAND ----------

# MAGIC %md
# MAGIC Density 그래프 그리기

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv"))


august.registerTempTable("august")

# COMMAND ----------

tmp = spark.sql("select track, count(distinct matchId) as cnt, collect_list(p_matchTime) as matchtime from august group by track order by 2")
tmp = tmp.filter(tmp.cnt>1000)

# COMMAND ----------

tmp.display()

# COMMAND ----------

from plotly.figure_factory import create_distplot
import numpy as np

# COMMAND ----------

tmp.collect()[0][1]

# COMMAND ----------

x0 = tmp.collect()[0][2]
x1 = tmp.collect()[1][2]
x2 = tmp.collect()[2][2]
x3 = tmp.collect()[3][2]
x4 = tmp.collect()[4][2]
x5 = tmp.collect()[5][2]
x6 = tmp.collect()[6][2]
x7 = tmp.collect()[7][2]
x8 = tmp.collect()[8][2]
x9 = tmp.collect()[9][2]

# COMMAND ----------

hist_data = [x0, x1, x2, x3, x4]
group_labels = [ tmp.collect()[0][0], tmp.collect()[1][0],tmp.collect()[2][0],tmp.collect()[3][0],tmp.collect()[4][0]]

# COMMAND ----------

df['track']

# COMMAND ----------

import pandas as pd
import numpy as np
df = pd.DataFrame({'2012': np.random.randn(200),
                   '2013': np.random.randn(200)+1})

# COMMAND ----------

[df[c] for c in df.columns]

# COMMAND ----------

df.describe()

# COMMAND ----------

### 테스트용 

fig = create_distplot([df[c] for c in df.columns], df.columns)
fig.show()

# COMMAND ----------

tmpdf = tmp.select("*").toPandas()
tmpdf.describe()

# COMMAND ----------

fig = create_distplot(
    hist_data, group_labels,
    show_rug=False, bin_size=.4)
fig.show()

# COMMAND ----------

fig = create_distplot(
    hist_data, group_labels, curve_type='normal',
    show_rug=False, bin_size=.4)
fig.show()

# COMMAND ----------



# COMMAND ----------

fig = create_distplot(
    hist_data, group_labels, curve_type='kde',
    show_rug=False, bin_size=.4)
fig.show()

# COMMAND ----------

### 5개만
fig = create_distplot(
    hist_data, group_labels, curve_type='normal',
    show_rug=False, bin_size=.4)
fig.show()

# COMMAND ----------

tmp.collect()[0][2]

# COMMAND ----------

tmp

# COMMAND ----------

hist_data = [[tmp_list1]]
group_labels = ['2012', '2013', '2014', '2015']
collect_list

# COMMAND ----------

df = tmp.select("*").toPandas()

# COMMAND ----------

tmp_list = tmp.select("track").rdd.flatMap(lambda x: x).collect()
tmp_list

# COMMAND ----------

tmp_list[0]

# COMMAND ----------

len(tmp_list)

# COMMAND ----------

tmp_list1 = tmp_list[:10]
tmp_list2 = tmp_list[10:20]
tmp_list3 = tmp_list[20:30]
tmp_list4 = tmp_list[30:40]
tmp_list5 = tmp_list[40:50]
tmp_list6 = tmp_list[50:60]
tmp_list7 = tmp_list[60:70]
tmp_list8 = tmp_list[70:80]
tmp_list9 = tmp_list[80:90]
tmp_list10 = tmp_list[90:100]

tmp_list11 = tmp_list[100:110]
tmp_list12 = tmp_list[110:120]
tmp_list13 = tmp_list[120:130]
tmp_list14 = tmp_list[130:]

# COMMAND ----------

tmp_list1[0]

# COMMAND ----------

len(tmp_list2)

# COMMAND ----------

tmp_list1

# COMMAND ----------

tmp_list2

# COMMAND ----------

hist_data = [[tmp_list1]]
group_labels = ['2012', '2013', '2014', '2015']
collect_list

# COMMAND ----------

from plotly.figure_factory import create_distplot
import numpy as np
import pandas as pd


fig = create_distplot(
    hist_data, group_labels, curve_type='normal',
    show_rug=False, bin_size=.4)



df = pd.DataFrame({'2012': np.random.randn(200),
                   '2013': np.random.randn(200)+1})
fig = create_distplot([df[c] for c in df.columns], df.columns)
fig.show()

# COMMAND ----------

## dataframe 10개씩 묶어서 split 하기. 
m.filter(m.cnt > 69000).show()

# COMMAND ----------

# m0 = m.filter(m.cnt > 69000)
# m1 =  m.filter((m.cnt > 47000) &( m.cnt< 69000))
# m2 =  m.filter((m.cnt > 23000) & (m.cnt< 47000))
# m3 =  m.filter((m.cnt > 14049) & (m.cnt< 23000))
# m4 =  m.filter((m.cnt > 10640) & (m.cnt< 14049))
# m5 =  m.filter((m.cnt > 7600) & (m.cnt< 10640))
# m6 =  m.filter((m.cnt > 6000) & (m.cnt< 7600))
# m7 =  m.filter((m.cnt > 4400) & (m.cnt< 6000))
# m8 =  m.filter((m.cnt > 3280) & (m.cnt< 4400))
# m9 =  m.filter((m.cnt > 2780) & (m.cnt< 3280))
# m10 =  m.filter((m.cnt > 2000) & (m.cnt< 2780))
# m11 =  m.filter((m.cnt > 1600) & (m.cnt< 2000) & (m.mean != 156.46140685461583))
m12 =  m.filter((m.cnt > 1200) & (m.cnt< 1700) & (m.track != '프로즌 브레이크'))
# m13 =  m.filter((m.cnt > 1000) & (m.cnt< 1200))

# COMMAND ----------

print(m0.count())
print(m1.count())
print(m2.count())
print(m3.count())
print(m4.count())
print(m5.count())
print(m6.count())
print(m7.count())
print(m8.count())
print(m9.count())
print(m10.count())
print(m11.count())
print(m12.count())
print(m13.count())

# COMMAND ----------

m11.display()

# COMMAND ----------

m12.display()

# COMMAND ----------

m12.display()

# COMMAND ----------

import plotly.figure_factory as ff
import numpy as np
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv"))


august.registerTempTable("august")

# COMMAND ----------

spark.sql("select min(startTime), max(startTime) from august").show()

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september4.csv"))


september.registerTempTable("september")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from september;