# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

### 빌리지 고가의 질주
village = spark.sql("select channelName, p_matchTime from august where track = '빌리지 고가의 질주'")
village.registerTempTable("village")

# COMMAND ----------

###채널별로 공통된 특성을 통해 묶을 수 있는 지 여부 파악
## One-way Anova test 이용 
village.display()

# COMMAND ----------

village.groupBy('channelName').count().display()

# COMMAND ----------

villagedf = village.select("*").toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC 멘토님 질문!!!

# COMMAND ----------

# MAGIC %md
# MAGIC 해결책 1 : boxplot 그리기 위해 채널별 나누기 시도. 

# COMMAND ----------

first = spark.sql("select * from village where channelName in ('speedTeamInfinit','tierMatching_speedTeam','speedTeamNewbie','grandprix_speedTeamInfinit')")

# COMMAND ----------

from chart_studio import plotly
import plotly.graph_objs as go
import pandas as pd
import requests
requests.packages.urllib3.disable_warnings()

# COMMAND ----------

firstdf = first.select("*").toPandas()

# COMMAND ----------

fig = px.box(firstdf, x="channelName", y="p_matchTime", color = "channelName", title="고가의 질주 channelName")


fig.show()

# COMMAND ----------

fig = px.box(firstdf, x="channelName", y="p_matchTime", color = "channelName", height = 800, title = '고가의 질주 channelName')
fig.update_yaxes(range=[0, 200], row=1, col=1)


fig.show()

# COMMAND ----------

second = spark.sql("select * from village where channelName in ('speedTeamCombine','speedTeamFast','speedTeamFastest','speedTeamFastNewbie')")

# COMMAND ----------

# MAGIC %md
# MAGIC second, third로 channel 나누기.

# COMMAND ----------

### 다시 나누기 
second = spark.sql("select * from village where channelName in ('speedTeamCombine','speedTeamFast')")
seconddf= second.select("*").toPandas()
fig = px.box(seconddf, x="channelName", y="p_matchTime", color = "channelName", title="고가의 질주 channelName")
fig.show()

# COMMAND ----------

fig = px.box(seconddf, x="channelName", y="p_matchTime", color = "channelName", height = 800, title = '고가의 질주 channelName')
fig.update_yaxes(range=[0, 200], row=1, col=1)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### One way Anova 검정 (channel - matchTime)

# COMMAND ----------

from anova_in_spark import one_way_anova
sswg, ssbg, f_statistic, df_1, df_2 = one_way_anova(df_of_anova, 'categorical_variable_column_name','continuous_variable_column_name')


# COMMAND ----------

from pyspark.sql.functions import lit, avg, count, udf, struct, sum
from pyspark.sql.types import DoubleType


def one_way_anova(df, categorical_var, continuous_var):
    """
    Given a Spark Dataframe, compute the one-way ANOVA using the given categorical and continuous variables.
    :param df: Spark Dataframe
    :param categorical_var: Name of the column that represents the grouping variable to use
    :param continuous_var: Name of the column corresponding the continuous variable to analyse
    :return: Sum of squares within groups, Sum of squares between groups, F-statistic, degrees of freedom 1, degrees of freedom 2
    """

    global_avg = df.select(avg(continuous_var)).take(1)[0][0]

    avg_in_groups = df.groupby(categorical_var).agg(avg(continuous_var).alias("Group_avg"),
                                                    count("*").alias("N_of_records_per_group"))
    avg_in_groups = avg_in_groups.withColumn("Global_avg",
                                             lit(global_avg))

    udf_between_ss = udf(lambda x: x[0] * (x[1] - x[2]) ** 2,
                         DoubleType())
    between_df = avg_in_groups.withColumn("squared_diff",
                                          udf_between_ss(struct('N_of_records_per_group',
                                                                'Global_avg',
                                                                'Group_avg')))
    ssbg = between_df.select(sum('squared_diff')).take(1)[0][0]

    within_df_joined = avg_in_groups \
        .join(df,
              df.channelName == avg_in_groups.channelName) \
        .drop(avg_in_groups.channelName)

    udf_within_ss = udf(lambda x: (x[0] - x[1]) ** 2, DoubleType())
    within_df_joined = within_df_joined.withColumn("squared_diff",
                                                   udf_within_ss(struct(continuous_var,
                                                                        'Group_avg')))
    sswg = within_df_joined \
        .groupby("channelName") \
        .agg(sum("squared_diff").alias("sum_of_squares_within_gropus")) \
        .select(sum('sum_of_squares_within_gropus')).take(1)[0][0]
    m = df.groupby('channelName') \
        .agg(count("*")) \
        .count()  # number of levels
    n = df.count()  # number of observations
    df1 = m - 1
    df2 = n - m
    f_statistic = (ssbg / df1) / (sswg / df2)
    return sswg, ssbg, f_statistic, df1, df2

# COMMAND ----------

sswg, ssbg, f_statistic, df1, df2 = one_way_anova(village, 'channelName','p_matchTime')
f_statistic

# COMMAND ----------

df2

# COMMAND ----------

# MAGIC %md
# MAGIC 번거롭지만 pandas 이용한 anova test. 

# COMMAND ----------

# MAGIC %r
# MAGIC age <- sql("SELECT * FROM village")

# COMMAND ----------

# MAGIC %r
# MAGIC head(age)

# COMMAND ----------

# MAGIC %md
# MAGIC ### R - anova 
# MAGIC Anova test R 사용해서 다시 시도!!! 제발 되어라라라아아아아앗

# COMMAND ----------

# MAGIC %r
# MAGIC library(sparklyr)
# MAGIC library(SparkR)

# COMMAND ----------

# MAGIC %r
# MAGIC villageR <- sql("SELECT * FROM village")
# MAGIC head(villageR)

# COMMAND ----------

# MAGIC %md
# MAGIC H0: The mean of salary in each group (e.g. MA represents mean of group A) is not significantly different (i.e. MA = MB = MC) </br>
# MAGIC An alternative (Ha) to this hypothesis will be the means are significantly different or at least one of the three means is significantly different from other two.

# COMMAND ----------

# MAGIC %r
# MAGIC library(ggpubr)

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

### 빌리지 고가의 질주
village = spark.sql("select channelName, p_matchTime from august where track = '빌리지 고가의 질주'")
village.registerTempTable("village")

# COMMAND ----------

# ### 빌리지 고가의 질주 (매치 중복 x) => p_matchTime 어차피 각자 선수라 매치 중복 제거 안함. 중복 된다 해도 개별 선수라 괜찮다 파악. 
# village = spark.sql("select channelName, (distinct matchid) as matchid,  p_matchTime from august where track = '빌리지 고가의 질주' group by matchid ")
# village.registerTempTable("village")

# COMMAND ----------

df = village.select("*").toPandas()

# COMMAND ----------

df['channelName'].value_counts(0)

# COMMAND ----------

#test = spark.sql("select channelName, collect_list(p_matchTime) as matchtime from village group by channelName")

# COMMAND ----------

speedTeamFastest = spark.sql("select p_matchTime from village where channelName = 'speedTeamFastest'")
speedTeamInfinit  = spark.sql("select p_matchTime from village where channelName = 'speedTeamInfinit'")

speedTeamFast = spark.sql("select p_matchTime from village where channelName = 'speedTeamFast'")
speedTeamCombine = spark.sql("select p_matchTime from village where channelName = 'speedTeamCombine'")
speedTeamFastNewbie = spark.sql("select p_matchTime from village where channelName = 'speedTeamFastNewbie'")
speedTeamNewbie = spark.sql("select p_matchTime from village where channelName = 'speedTeamNewbie'")
tierMatching_speedTeam = spark.sql("select p_matchTime from village where channelName = 'tierMatching_speedTeam'")
grandprix_speedTeamInfinit = spark.sql("select p_matchTime from village where channelName = 'grandprix_speedTeamInfinit'")

# COMMAND ----------

speedTeamFastest= speedTeamFastest.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamInfinit= speedTeamInfinit.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamFast= speedTeamFast.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamCombine= speedTeamCombine.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamFastNewbie= speedTeamFastNewbie.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamNewbie= speedTeamNewbie.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
tierMatching_speedTeam= tierMatching_speedTeam.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
grandprix_speedTeamInfinit= grandprix_speedTeamInfinit.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

### 예시 확인 
speedTeamFastest

# COMMAND ----------

### Statsmodel을 사용한 일원분산분석¶
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm

#df = pd.DataFrame(data, columns=['value','treatment'])
print(df.head(3))

model = ols('p_matchTime ~ C(channelName)', df).fit()
print(anova_lm(model))


# COMMAND ----------

df['channelName'].value_counts()

# COMMAND ----------

######### anova 하기 위한 조건 확인 (독립성은....... 만족한다 판단했습니다)
#### p-value 값이 너무 커 서칭하던 중, anova 모델을 위한 기본 조건이 불충분해서 그런가 고민이 되어 확인했습니다!

#정규성 확인 - 집단(수준)별로 실시  => 각 집단은 정규성을 만족합니당 ㅎㅎ

from  scipy.stats import shapiro
print(shapiro(speedTeamFastest))
print(shapiro(speedTeamInfinit))
print(shapiro(speedTeamFast))

print(shapiro(speedTeamCombine))
print(shapiro(speedTeamFastNewbie))
print(shapiro(speedTeamNewbie))

print(shapiro(tierMatching_speedTeam))
print(shapiro(grandprix_speedTeamInfinit))


# COMMAND ----------

### 등분산성 확인 - 레빈 검증
from scipy.stats import levene
print(levene(speedTeamFastest,
      speedTeamInfinit,
      speedTeamFast,
           speedTeamCombine,
            speedTeamFastNewbie,
            speedTeamNewbie,
            tierMatching_speedTeam,
            grandprix_speedTeamInfinit))

### 결과 :8 집단의 모분산에 유의미한 차이를 발견하지 못함. 등분산성 가정이 유지됨

# COMMAND ----------

### 등분산성 확인 - 바틀렛 검증
### 결과 : .... ㅠㅠㅠㅠ 8집단의 모분산에 유의미한...차이가 있음 ㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠ 등분산성 가정 유지.. 안됨.. =>  Welch’s ANOVA???? using python.....................??
from scipy.stats import bartlett
print(bartlett(speedTeamFastest,
      speedTeamInfinit,
      speedTeamFast,
           speedTeamCombine,
            speedTeamFastNewbie,
            speedTeamNewbie,
            tierMatching_speedTeam,
            grandprix_speedTeamInfinit))


# COMMAND ----------

print(bartlett(speedTeamFastest,
      speedTeamInfinit,
      speedTeamFast,
           speedTeamCombine,
            speedTeamFastNewbie,
            speedTeamNewbie,
            tierMatching_speedTeam))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welch's ANOVA

# COMMAND ----------

import pingouin as pg
import pandas as pd
import numpy as np

# COMMAND ----------

aa = pd.DataFrame({'score': [64, 66, 68, 75, 78, 94, 98, 79, 71, 80,
                             91, 92, 93, 90, 97, 94, 82, 88, 95, 96,
                             79, 78, 88, 94, 92, 85, 83, 85, 82, 81],
                   'group': np.repeat(['a', 'b', 'c'], repeats=10)}) 
aa

# COMMAND ----------

#perform Welch's ANOVA
pg.welch_anova(dv='p_matchTime', between='channelName', data=df)
### 기각 성공!!!!!!!!! (0.0 < 0.05)

# COMMAND ----------

### 사후 분석
pg.pairwise_gameshowell(dv='p_matchTime', between='channelName', data=df)

# COMMAND ----------

### 결과를 통해 상당히 다른 애들 파악 : 
###### [grandprix_speedTeamInfinit,	speedTeamFastNewbie]
###### [grandprix_speedTeamInfinit,	speedTeamNewbie	]
###### [speedTeamCombine	speedTeamFastNewbie]
###### [speedTeamCombine	speedTeamFastest]
###### [speedTeamCombine	speedTeamInfinit]
###### [speedTeamCombine	speedTeamNewbie]
###### [speedTeamCombine	tierMatching_speedTeam]
###### [speedTeamFastNewbie	speedTeamFastest]
###### [speedTeamFastNewbie	speedTeamInfinit]
###### [speedTeamFastNewbie	speedTeamNewbie]
###### [speedTeamFastNewbie	tierMatching_speedTeam]
###### [speedTeamFastest	speedTeamInfinit	]
###### [speedTeamFastest	speedTeamNewbie	]
###### [speedTeamFastest	tierMatching_speedTeam	]
###### [speedTeamInfinit	speedTeamNewbie	]
###### [speedTeamInfinit	tierMatching_speedTeam	]
###### [speedTeamNewbie	tierMatching_speedTeam	]

# COMMAND ----------

# MAGIC %md
# MAGIC 시도한 코드들 

# COMMAND ----------

import scipy.stats as stats
F_statistic, pVal = stats.f_oneway(speedTeamFastest, speedTeamInfinit, speedTeamFast, speedTeamCombine, speedTeamFastNewbie,speedTeamNewbie,tierMatching_speedTeam, grandprix_speedTeamInfinit)

print('channel별 matchTime에 대한 일원분산분석 결과 : F={0:.1f}, p={1:.5f}'.format(F_statistic, pVal))
if pVal < 0.05:
    print('P-value 값이 충분히 작음으로 인해 그룹의 평균값이 통계적으로 유의미하게 차이납니다.')

# COMMAND ----------

##### 빌리지 고가의 질주 내 모든 채널의 평균간에 차이가 없다.......????????

# COMMAND ----------

fvalue, pvalue = stats.f_oneway(speedTeamFastest, speedTeamInfinit, speedTeamFast, speedTeamCombine, speedTeamFastNewbie,speedTeamNewbie,tierMatching_speedTeam)
print(fvalue, pvalue)

# COMMAND ----------

fvalue, pvalue = stats.f_oneway(speedTeamFastest, speedTeamInfinit, speedTeamFast, speedTeamCombine, speedTeamFastNewbie)
print(fvalue, pvalue)

# COMMAND ----------

### https://www.reneshbedre.com/blog/anova.html

# COMMAND ----------

import scipy.stats as stats
# stats f_oneway functions takes the groups as input and returns ANOVA F and p value
fvalue, pvalue = stats.f_oneway(df['A'], df['B'], df['C'], df['D'])
print(fvalue, pvalue)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 인원수-채널별로 랩타임 anova (two-way anova)  ===> 이원분석에서 등분산성 문제!!!!!!!!!!!(미해결상태)  근데 로그 트랜스퍼를 여기서 시키면 안되는 거 아닌가.... 일단 회귀 모델 돌린 후 back 해야하나...... 

# COMMAND ----------

import pandas as pd
import numpy as np
import urllib
import matplotlib.pyplot as plt

# COMMAND ----------

village2 = spark.sql("select channelName, teamPlayers, p_matchTime from august where track = '빌리지 고가의 질주'")
village2.display()

# COMMAND ----------

village2.count()

# COMMAND ----------

df2 = village2.select("*").toPandas()
df2.boxplot(column='p_matchTime', by='channelName')
plt.show()

# COMMAND ----------

df2.boxplot(column='p_matchTime', by='teamPlayers')
plt.show()

# COMMAND ----------

august

# COMMAND ----------

### 
import pandas as pd
import statsmodels.formula.api as sm
from statsmodels.sandbox.regression.predstd import wls_prediction_std

# 회귀분석 수행
mdl = sm.ols(formula='p_matchTime~channelName+teamPlayers', data=df2)
# 회귀분석 결과에서 잔차만 추출
resid = mdl.fit().resid



# COMMAND ----------

from scipy.stats import probplot
plt.figure()
probplot(resid, plot=plt)
plt.show()
###...... 하..... 

# COMMAND ----------

from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm

formula = 'p_matchTime ~ C(channelName)+C(teamPlayers)+C(channelName):C(teamPlayers)'
lm = ols(formula, df2).fit()
print(anova_lm(lm))

# COMMAND ----------

anova_lm(lm
         
### p-value가 0.05이상. 귀무가설을 기각할 수 없고 channelName과 teamPlayers의 수는 연관성이 없다고 할 수 있다.
### channelName에 따라 평균에 차이가 없음. 
### teamplayers 명 수에 따라 평균에 차이가 없음.
### channelName에 따른 teamPlayers 간에 차이가 없음. ... 

# COMMAND ----------

import numpy as np
df2.boxplot(column = ['p_matchTime'], by=['channelName', 'teamPlayers'])

# COMMAND ----------

aa

# COMMAND ----------

df2

# COMMAND ----------

from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm

model = ols('p_matchTime ~ C(channelName)+C(teamPlayers)+C(channelName):C(teamPlayers)', df2).fit()

# COMMAND ----------

anova_lm(model, typ=3) #비균형 자료라 3을 추가.  ==> 근데 아닌 듯 다음꺼인듯. 

# COMMAND ----------

model = ols('p_matchTime ~ C(channelName)+C(teamPlayers)', df2).fit()
anova_lm(model, typ=3)

# COMMAND ----------



# COMMAND ----------

### 정규성, 등분산
### 비균형 이원 분석 
from scipy.stats import levene

# Create three arrays for each sample:
ctrl = df2.query('channelName == "ctrl"')['weight']
trt1 = df2.query('group == "trt1"')['weight']
trt2 = df2.query('group == "trt2"')['weight']

# Levene's Test in Python with Scipy:
stat, p = levene(ctrl, trt1, trt2)

print(stat, p)

# COMMAND ----------

# MAGIC %md
# MAGIC ### matchTime * playTime 상관분석

# COMMAND ----------

village4 = spark.sql("select channelName, p_matchTime, playTime, teamPlayers from august where track ='빌리지 고가의 질주'")

# COMMAND ----------

df4 = village4.select("*").toPandas()

# COMMAND ----------

X = df4.playTime.values
Y = df4.p_matchTime.values

# COMMAND ----------

import matplotlib.pyplot as plt
plt.scatter(X, Y, alpha = 0.5)
plt.title('p_matchTime ~ playTime')
plt.xlabel('playTime')
plt.ylabel('p_matchTime')
plt.show() ###### 산점도를 통해 보았을 때는 .... 딱히 관계가 보이지... 않습니다ㅠ 

# COMMAND ----------

### 공분산!!!

cov = (np.sum(X*Y) - len(X)*np.mean(X)*np.mean(Y))/len(X)
cov

# COMMAND ----------

np.cov(X, Y)[0,1]

# COMMAND ----------

np.corrcoef(X,Y)[0,1] ####..... playTime을 하나의 변수로 사용하는 거 괜찮을 것 같습니다. 

# COMMAND ----------

import scipy.stats as stats
#### H0 :  matchtime과 playtime은 상관관계가 없다 
#### H1 : 상관관계가 있다 
stats.pearsonr(X, Y)      ####(뒤에 결과값 : p-value)
### 결과 해석 : 상관 관계가 있기는 하나 매우 약하다. 따라서, playtime은 matchtime과 아주 약간의 상관 관계가 있으므로, 다중공선성을 걱정할 필요가 없다. 

# COMMAND ----------

/FileStore/KartRider/scaled_encoded.csv

# COMMAND ----------

aaa = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/scaled_encoded.csv"))

aaa.registerTempTable("aaa")

# COMMAND ----------

aaa.display()