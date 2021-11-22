# Databricks notebook source
#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/all_collected_processed.csv"))


final.registerTempTable("finalsql")

# COMMAND ----------

spark.sql("select * from finalsql").display()

# COMMAND ----------

final.groupBy('p_partsEngine').count().orderBy('count').show()

# COMMAND ----------

yy = spark.sql("select p_partsEngine, p_matchTime from finalsql")
yy = yy.select("*").toPandas()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['axes.unicode_minus'] = False ## 마이나스 '-' 표시 제대로 출력
 
from statsmodels.formula.api import ols
from sklearn.linear_model import LinearRegression

# COMMAND ----------

final.groupBy('p_kart').count().orderBy('count').show()

# COMMAND ----------

final.groupBy('name').count().orderBy('count').show()

# COMMAND ----------

yy2 = spark.sql("select name, p_matchTime from finalsql")
yy2 = yy2.select("*").toPandas()

# COMMAND ----------

## 시각화
fig = plt.figure(figsize=(8,8))
fig.set_facecolor('white')
 
font_size = 15
plt.scatter(yy2['name'],yy2['p_matchTime']) ## 원 데이터 산포도

 
plt.xlabel('Lot Size', fontsize=font_size)
plt.ylabel('Work Hours',fontsize=font_size)
plt.show()

# COMMAND ----------

# 전체 데이터 개수 확인 
print((final.count(), len(final.columns)))

# COMMAND ----------

### p_matchTime null 개수 확인 
spark.sql("select count(*) from finalsql where p_matchTime is Null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark로! 이따 코드 변환시켜버리기

# COMMAND ----------

# track별로 matchTime 확인
test1 = spark.sql("select track, collect_list(p_matchTime) as matchtime from finalsql group by track")
test1.display()

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

slen = udf(lambda s: len(s), IntegerType())

test1 = test1.withColumn("cnt", slen(test1.matchtime))
test1.show()

# COMMAND ----------

test1.display()

# COMMAND ----------

test1.registerTempTable("test1sql")

# COMMAND ----------

spark.sql("select * from test1sql order by cnt").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from test1sql limit 10

# COMMAND ----------

spark.sql("select * from test1sql order by cnt desc limit 10").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC As for 빌리지 고가의 질주

# COMMAND ----------

spark.sql("select matchtime from test1sql where track == '[reverse] 빌리지 고가의 질주' ").display()

# COMMAND ----------

# reverse 변환....... 고민. 
onlymatchtime = spark.sql("select matchtime from test1sql where track == '[reverse] 빌리지 고가의 질주' ")

# COMMAND ----------

type(onlymatchtime.select("matchtime").collect())

# COMMAND ----------

onlymatchtime.select(onlymatchtime.matchtime[1]).show()

# COMMAND ----------

data = onlymatchtime.select("matchtime").collect()

# COMMAND ----------

import seaborn as sns
sns.distplot(onlymatchtime.select("matchtime").collect(), hist=True, kde=True, 
             bins=int(180/5), color = 'darkblue', 
             hist_kws={'edgecolor':'black'},
             kde_kws={'linewidth': 4})

# COMMAND ----------

import matplotlib.pyplot as plt
sns.distplot(data, kde=True, rug=True, color="red")
plt.show()


# COMMAND ----------

from pyspark.mllib.stat import KernelDensity

data =onlymatchtime.select("matchtime").collect()

kd = KernelDensity()
kd.setSample(data)
kd.setBandwidth(3.0)

# COMMAND ----------

# track별로 matchTime 확인 & density 함수! 
# https://artist-developer.tistory.com/17
spark.sql("select track from finalsql")

# COMMAND ----------

# MAGIC %md
# MAGIC 2021-10-16

# COMMAND ----------

#spark.sql("select matchtime from test1sql where track == '[reverse] 빌리지 고가의 질주' ").display()


# COMMAND ----------

# MAGIC %md
# MAGIC 빌리지 고가의 질주 

# COMMAND ----------

village = spark.sql("select * from finalsql where track = '[reverse] 빌리지 고가의 질주'")

# COMMAND ----------

village.registerTempTable("villagesql")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select channelName, count(*)
# MAGIC from villagesql
# MAGIC group by channelName

# COMMAND ----------

villagesql.groupBy('channelName').collect()

# COMMAND ----------

temp = spark.sql("select channelName, collect_list(p_matchTime) as matchtime from villagesql group by channelName")
temp.registerTempTable("tempsql")

# COMMAND ----------

temp.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tempsql

# COMMAND ----------

speedTeamInfinit = spark.sql("select matchtime from tempsql where channelName == 'speedTeamInfinit'")
speedTeamInfinit= speedTeamInfinit.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamInfinit = speedTeamInfinit[0]

# COMMAND ----------

speedTeamInfinit = speedTeamInfinit.select('matchtime').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

speedTeamInfinit = speedTeamInfinit[0]
speedTeamInfinit.summary().show()

# COMMAND ----------

import numpy as np
np.quantile(speedTeamInfinit, 0.5)

# COMMAND ----------

temp.summary().show()

# COMMAND ----------

spark.sql("select matchtime from villagesql where channelName == 'speedTeamInfinit' ").

# COMMAND ----------

temp.select('mvv').collect()

# COMMAND ----------

#고가의 질주 안에서 채널별 matchTime에 대한 차이가 있는 지 여부 확인
import seaborn as sns 

ax = sns.boxplot(x="channelName", y="matchTime", data=village)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select channelName, count(*) 
# MAGIC from finalsql 
# MAGIC where track = '[reverse] 빌리지 고가의 질주'
# MAGIC group by channelName;

# COMMAND ----------

# MAGIC %md
# MAGIC 채널별 groupping 필요성 확인 

# COMMAND ----------

# MAGIC %md
# MAGIC speedTeamInfint 내 비교

# COMMAND ----------

example = spark.sql("select * from villagesql where channelName = 'speedTeamInfinit'")
example.show()

# COMMAND ----------

import scipy.stats as stats
import pandas as pd
import urllib
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
import matplotlib.pyplot as plt
import numpy as np
%matplotlib inline

# COMMAND ----------

example.groupBy('teamPlayers').count().orderBy('count').show()

# COMMAND ----------

example.registerTempTable('examplesql')
one = spark.sql("select p_matchTime from examplesql where teamPlayers = 1")
two = spark.sql("select p_matchTime from examplesql where teamPlayers = 2")
three = spark.sql("select p_matchTime from examplesql where teamPlayers = 3")
four = spark.sql("select p_matchTime from examplesql where teamPlayers = 4")

# COMMAND ----------

one.registerTempTable('onesql')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from onesql

# COMMAND ----------

one= one.select('p_matchTime').rdd.flatMap(lambda x: x).collect()


# COMMAND ----------

one

# COMMAND ----------

print(type(one))
print(type(one[0]))

# COMMAND ----------

two= two.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
three= three.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
four= four.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

one[0]+one[1]

# COMMAND ----------

two

# COMMAND ----------

Not_none_values = filter(None.__ne__, list_of_values)

list_of_values = list(Not_none_values)

# COMMAND ----------

Not_none_values = filter(None.__ne__, one)
one = list(Not_none_values)

# COMMAND ----------

Not_none_values = filter(None.__ne__, two)
two = list(Not_none_values)
Not_none_values = filter(None.__ne__, three)
three = list(Not_none_values)
Not_none_values = filter(None.__ne__, four)
four = list(Not_none_values)

# COMMAND ----------

plot_data = [one, two, three,four]
ax = plt.boxplot(plot_data)
plt.show()

# COMMAND ----------

F_statistic, pVal = stats.f_oneway(one, two, three, four)

print('Altman 910 데이터의 일원분산분석 결과 : F={0:.1f}, p={1:.5f}'.format(F_statistic, pVal))
if pVal < 0.05:
    print('P-value 값이 충분히 작음으로 인해 그룹의 평균값이 통계적으로 유의미하게 차이납니다.')

# COMMAND ----------

f_oneway(one, two, three,four)

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy.stats import f_oneway
from statsmodels.stats.multicomp import pairwise_tukeyhsd

# COMMAND ----------

len(three)

# COMMAND ----------

df = pd.DataFrame(list(zip(one, two, three, four)), columns =['one', 'two', 'three', 'four']) 
print(df) 

# COMMAND ----------

df = pd.DataFrame(two)
df = pd.DataFrame(three)
df = pd.DataFrame(four)
df

# COMMAND ----------

df2 = pd.DataFrame({'one': pd.Series(one), 'two': pd.Series(two), 'three': pd.Series(three),'four': pd.Series(four)})

# COMMAND ----------

df2

# COMMAND ----------

df3 = pd.DataFrame({'treatment':'one', 'value':pd.Series(one)})
df3

# COMMAND ----------

df4 = pd.DataFrame({'treatment':'two',  'value':pd.Series(two)})
df5 = pd.DataFrame({'treatment':'three',  'value':pd.Series(three)})
df6 = pd.DataFrame({'treatment':'four',  'value':pd.Series(four)})

# COMMAND ----------

df7 = pd.concat([df3, df4, df5, df6], axis=0)

# COMMAND ----------

df7

# COMMAND ----------

from statsmodels.stats.multicomp import pairwise_tukeyhsd

posthoc = pairwise_tukeyhsd(df7['value'], df7['treatment'], alpha=0.05)
print(posthoc)

# COMMAND ----------

# 경고 메세지 무시하기
import warnings
warnings.filterwarnings('ignore')

df = pd.DataFrame(data, columns=['value', 'treatment'])    

# the "C" indicates categorical data
model = ols('value ~ C(treatment)', df).fit()

print(anova_lm(model))

# COMMAND ----------

채널과 players 수 모두 독립변수로 넣어서 two-way anova 해볼까요? 지금까지로 봐서 변수 저렇게 각자 넣어야할거같긴해서용

# COMMAND ----------

speedTeamInfinit = spark.sql("select matchtime from tempsql where channelName == 'speedTeamInfinit'")
speedTeamInfinit= speedTeamInfinit.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamInfinit = speedTeamInfinit[0]

# COMMAND ----------

speedTeamNewbie= spark.sql("select matchtime from tempsql where channelName == 'speedTeamNewbie'")
speedTeamNewbie= speedTeamNewbie.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamNewbie = speedTeamNewbie[0]

# COMMAND ----------

speedTeamFast= spark.sql("select matchtime from tempsql where channelName == 'speedTeamFast'")
speedTeamFast= speedTeamFast.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamFast = speedTeamFast[0]

# COMMAND ----------

speedTeamFastest= spark.sql("select matchtime from tempsql where channelName == 'speedTeamInfinit'")
speedTeamFastest= speedTeamFastest.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamFastest = speedTeamFastest[0]

# COMMAND ----------

speedTeamFastNewbie= spark.sql("select matchtime from tempsql where channelName == 'speedTeamInfinit'")
speedTeamFastNewbie= speedTeamFastNewbie.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamFastNewbie = speedTeamFastNewbie[0]

# COMMAND ----------

speedTeamCombine= spark.sql("select matchtime from tempsql where channelName == 'speedTeamInfinit'")
speedTeamCombine= speedTeamCombine.select('matchtime').rdd.flatMap(lambda x: x).collect()
speedTeamCombine = speedTeamCombine[0]

# COMMAND ----------

plot_data = [speedTeamInfinit, speedTeamNewbie, speedTeamFast,speedTeamFastest,speedTeamFastNewbie, speedTeamCombine]
ax = plt.boxplot(plot_data)
plt.show()

# COMMAND ----------

df1 = pd.DataFrame({'treatment':'1', 'value':pd.Series(speedTeamInfinit)})
df2 = pd.DataFrame({'treatment':'2', 'value':pd.Series(speedTeamInfinit)})
df3 = pd.DataFrame({'treatment':'3', 'value':pd.Series(speedTeamInfinit)})
df4 = pd.DataFrame({'treatment':'4', 'value':pd.Series(speedTeamInfinit)})
df5 = pd.DataFrame({'treatment':'5', 'value':pd.Series(speedTeamFastNewbie)})
df6 = pd.DataFrame({'treatment':'6', 'value':pd.Series(speedTeamCombine)})