# Databricks notebook source
!pip install plotly chart_studio --upgrade

# COMMAND ----------

!pip install cufflinks --upgrade

# COMMAND ----------

pip install --upgrade pip

# COMMAND ----------

import chart_studio.plotly as py 
import seaborn as sns
import plotly.express as px
%matplotlib inline

from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot 
init_notebook_mode(connected=True)
cf.go_offline()

# COMMAND ----------

final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv"))

final.registerTempTable("final_df")

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

import pyspark 
from pyspark.sql.functions import *

spark.sql("select * from final_df").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, count(p_rankinggrade2) from final_df group by p_rankinggrade2 order by p_rankinggrade2").display()

# COMMAND ----------

spark.sql("select channelName, count(channelName) from final_df group by channelName order by channelName").display()

# COMMAND ----------

import chart_studio.plotly as py
import cufflinks as cf
cf.go_offline(connected=True)

# COMMAND ----------

cf.help('heatmap')

# COMMAND ----------

final.toPandas()

# COMMAND ----------

final2 = final
final2

# COMMAND ----------

final2.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### EDA #01 : Channel별 Rankinggrade

# COMMAND ----------

test = spark.sql("select channelName, p_rankinggrade2, count(p_rankinggrade2) as rankinggrade2count from final_df group by channelName, p_rankinggrade2 order by channelName, p_rankinggrade2 ")
test.display()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

tt = test.select("*").toPandas()
ttt = pd.DataFrame(tt.groupby(['channelName', 'p_rankinggrade2']).rankinggrade2count.sum())
ttt = ttt.reset_index()
ttt.display()

# COMMAND ----------

#tips.pivot_table("tip_pct", "sex", "smoker", aggfunc="count", margins=True)
aaa = ttt.pivot_table("rankinggrade2count", 'channelName', 'p_rankinggrade2', aggfunc="sum", margins=True, margins_name="합계")
aaa

# COMMAND ----------

aaa.describe()

# COMMAND ----------

copy = ttt.pivot(index ='channelName', columns = 'p_rankinggrade2', values = 'rankinggrade2count')
copy.display()

# COMMAND ----------

copy

# COMMAND ----------

copy[(copy.index == 'speedTeamFastNewbie') | (copy.index == 'speedTeamNewbie')]

# COMMAND ----------

copy.plot.barh(stacked=True, figsize=(20,10))

# COMMAND ----------

ttt.pivot_table(index='channelName', columns='p_rankinggrade2', aggfunc='size').plot.barh(stacked=True)

# COMMAND ----------

# fig 크기가 안 커짐.
# Stacked Bar Chart by pandas
import matplotlib.pyplot as plt

copy.plot.bar(stacked=True, rot=0)
plt.title('channelName-License', fontsize=20)
plt.figure(figsize=(20, 10))
plt.show()

# COMMAND ----------

print(copy.index)

# COMMAND ----------

tmp = copy.index.tolist()

# COMMAND ----------

tmp

# COMMAND ----------

copy[0]

# COMMAND ----------

print(copy.iplot(kind='bar', barmode='stack'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### EDA 와 시간 관계

# COMMAND ----------

#channel별 playtime을 날짜별로 확인
spark.sql("select * from final_df").display()

# COMMAND ----------

answer = spark.sql("select channelName, startTime_day, count(startTime_day) as cnt from final_df group by channelName, startTime_day order by startTime_day")
answer.select("*").toPandas()
answer.display()

# COMMAND ----------

answer.describe()

# COMMAND ----------

ppt = spark.sql("select p_rankinggrade2,startTime_hour, count(startTime_hour) as cnt from final_df group by p_rankinggrade2, startTime_hour order by startTime_hour, p_rankinggrade2")
ppt.select("*").toPandas()
ppt.display()

# COMMAND ----------

www = spark.sql("select p_rankinggrade2, count(startTime_hour) as cnt from final_df group by p_rankinggrade2, startTime_hour order by startTime_hour, p_rankinggrade2")
www.select("*").toPandas()
www.display()

# COMMAND ----------

result = spark.sql("select channelName,startTime_hour, count(startTime_hour) as cnt from final_df group by channelName, startTime_hour")
result.select("*").toPandas()
result.display()

# COMMAND ----------

result.describe().display()

# COMMAND ----------

burger = spark.sql("select p_rankinggrade2,startTime_hour, count(startTime_hour) as cnt from final_df group by p_rankinggrade2, startTime_hour")
burger.select("*").toPandas()
burger.display()

# COMMAND ----------

burger.describe().display()

# COMMAND ----------

# H0 : 시간대별로 등급에 차이가 있다.  vs H1 : 시간대별로 등급에 차이가 없다 
from scipy.stats import chisquare

result = chisquare(xo, f_exp=xe)
result

# COMMAND ----------

kkk = spark.sql("select channelName, sum(p_matchRetired)/count(p_matchRetired) as rate from final_df group by channelName order by channelName")
kkk.display()

# COMMAND ----------

mmm = spark.sql("select p_rankinggrade2, sum(p_matchRetired)/count(p_matchRetired) as rate from final_df group by p_rankinggrade2 order by p_rankinggrade2")
mmm.display()

# COMMAND ----------

lll = spark.sql("select * from final_df where trackName = '빌리지 고가의 질주'")
lll.display()

# COMMAND ----------

lll.registerTempTable('lll')

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 0").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 1").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 2").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 3").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 4").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 5").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 6").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 0").describe().display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 1").describe().display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 2").describe().display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll group by p_rankinggrade2, playTime").describe().display() #playTime 이 groupby 로 갔기 때문에 오답일 가능성 높음

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 3").describe().display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 4").describe().display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 5").describe().display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 6").describe().display()

# COMMAND ----------

print((final.count(), len(final.columns)))

# COMMAND ----------

ss = spark.sql("select p_rankinggrade2, playTime from lll")
ss.display()

# COMMAND ----------

ss.describe().display()

# COMMAND ----------

qq = spark.sql("select p_rankinggrade2, playTime from lll group by p_rankinggrade2, playTime order by p_rankinggrade2, playTime")
qq.display()

# COMMAND ----------

s0.display()

# COMMAND ----------

ss.registerTempTable('ss')

# COMMAND ----------

s0 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 0")
s1 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 1")
s2 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 2")
s3 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 3")
s4 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 4")
s5 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 5")
s6 = spark.sql("select p_rankinggrade2, playTime from lll where p_rankinggrade2 = 6")

# COMMAND ----------

my_list = s0.select("playtime").rdd.flatMap(lambda x: x).collect()
my_list

# COMMAND ----------

s0.select("*").toPandas()

# COMMAND ----------

df._get_value(index,'name')

# COMMAND ----------

ppp = spark.sql("select channelName, startTime_day, count(startTime_day) as cnt from final_df where channelName = 'grandprix_speedTeamInfinit' group by channelName, startTime_day")
ppp.select("*").toPandas()
ppp.display()

# COMMAND ----------

sss = spark.sql("select channelName, startTime_day, count(startTime_day) as cnt from final_df where channelName = 'speedTeamFast' group by channelName, startTime_day")
sss.select("*").toPandas()
sss.display()

# COMMAND ----------

print(copy.index)