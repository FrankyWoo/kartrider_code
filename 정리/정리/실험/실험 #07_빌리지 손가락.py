# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

### 빌리지 고가의 질주
china = spark.sql("select channelName, p_matchTime from august where track = '빌리지 손가락'")
china.registerTempTable("china")

# COMMAND ----------

china.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 채널별로 공통된 특성을 통해 묶을 수 있는 지 여부 파악
# MAGIC 
# MAGIC One-way Anova test 이용

# COMMAND ----------

china.groupBy('channelName').count().display()

# COMMAND ----------

chinadf = china.select("*").toPandas()

# COMMAND ----------

from chart_studio import plotly
import plotly.graph_objs as go
import pandas as pd
import requests
import plotly.express as px
requests.packages.urllib3.disable_warnings()

# COMMAND ----------

fig = px.box(chinadf, x="channelName", y="p_matchTime", color = "channelName", height = 800, title = '빌리지 손가락 channelName')
fig.update_yaxes(range=[0, 200], row=1, col=1)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### One way Anova 검정 (channel - matchTime)

# COMMAND ----------

speedTeamFastest = spark.sql("select p_matchTime from china where channelName = 'speedTeamFastest'")
speedTeamInfinit  = spark.sql("select p_matchTime from china where channelName = 'speedTeamInfinit'")

speedTeamFast = spark.sql("select p_matchTime from china where channelName = 'speedTeamFast'")
speedTeamCombine = spark.sql("select p_matchTime from china where channelName = 'speedTeamCombine'")
speedTeamFastNewbie = spark.sql("select p_matchTime from china where channelName = 'speedTeamFastNewbie'")
speedTeamNewbie = spark.sql("select p_matchTime from china where channelName = 'speedTeamNewbie'")
#tierMatching_speedTeam = spark.sql("select p_matchTime from china where channelName = 'tierMatching_speedTeam'")
grandprix_speedTeamInfinit = spark.sql("select p_matchTime from china where channelName = 'grandprix_speedTeamInfinit'")

# COMMAND ----------

speedTeamFastest= speedTeamFastest.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamInfinit= speedTeamInfinit.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamFast= speedTeamFast.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamCombine= speedTeamCombine.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamFastNewbie= speedTeamFastNewbie.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
speedTeamNewbie= speedTeamNewbie.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
#tierMatching_speedTeam= tierMatching_speedTeam.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
grandprix_speedTeamInfinit= grandprix_speedTeamInfinit.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

######### anova 하기 위한 조건 확인 (독립성은....... 만족한다 판단했습니다)
#### p-value 값이 너무 커 서칭하던 중, anova 모델을 위한 기본 조건이 불충분해서 그런가 고민이 되어 확인했습니다!

#정규성 확인 - 집단(수준)별로 실시  
from  scipy.stats import shapiro
print(shapiro(speedTeamFastest))
print(shapiro(speedTeamInfinit))
print(shapiro(speedTeamFast))

print(shapiro(speedTeamCombine))
print(shapiro(speedTeamFastNewbie))
print(shapiro(speedTeamNewbie))

#print(shapiro(tierMatching_speedTeam))
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
           # tierMatching_speedTeam,
            grandprix_speedTeamInfinit))
### 결과 :각 집단의 모분산에 유의미한 차이를 발견하지 못함. 등분산성 가정이 유지됨

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
            #tierMatching_speedTeam,
            grandprix_speedTeamInfinit))


# COMMAND ----------

print(bartlett(speedTeamFastest,
      speedTeamInfinit,
      speedTeamFast,
           speedTeamCombine,
            speedTeamFastNewbie,
            speedTeamNewbie))

# COMMAND ----------

### Statsmodel을 사용한 일원분산분석¶ ### 결과값이 나타나기는 하지만........ 오류의 가능성이.. 높습니다ㅠㅠㅠㅠ 
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm

#df = pd.DataFrame(data, columns=['value','treatment'])
print(chinadf.head(3))

model = ols('p_matchTime ~ C(channelName)', chinadf).fit()
print(anova_lm(model))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welch's ANOVA

# COMMAND ----------

import pingouin as pg
import pandas as pd
import numpy as np

pg.welch_anova(dv='p_matchTime', between='channelName', data=chinadf)

# COMMAND ----------

### 정규성 성립하지 않는 채널 제외!!! #speedTeamNewbie, grandprix_speedTeamInfinit
df = spark.sql("select * from china where channelName != 'speedTeamNewbie' and channelName != 'grandprix_speedTeamInfinit'")
pandas_df = df.select("*").toPandas()

# COMMAND ----------

#perform Welch's ANOVA
### 오오오!!! 기각 성공! 따라서 pandas_df 의 채널들 간에는 유의미한 차이가 존재한당
pg.welch_anova(dv='p_matchTime', between='channelName', data=pandas_df)


# COMMAND ----------

### 사후 분석 ###모든 애들 간에..... 다르댱........... 
pg.pairwise_gameshowell(dv='p_matchTime', between='channelName', data=pandas_df)

# COMMAND ----------

pandas_df['channelName'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### two-way anova 

# COMMAND ----------

import pandas as pd
import numpy as np
import urllib
import matplotlib.pyplot as plt

# COMMAND ----------

china2 = spark.sql("select channelName, teamPlayers, p_matchTime from august where track = '빌리지 손가락'")
china2.display()

# COMMAND ----------

china2.count()

# COMMAND ----------

df3 = china2.select("*").toPandas()
df3.boxplot(column='p_matchTime', by='channelName')
plt.show()

# COMMAND ----------

df3.boxplot(column='p_matchTime', by='teamPlayers')
plt.show()

# COMMAND ----------

### 
import pandas as pd
import statsmodels.formula.api as sm
from statsmodels.sandbox.regression.predstd import wls_prediction_std

# 회귀분석 수행
mdl = sm.ols(formula='p_matchTime~channelName+teamPlayers', data=df3)
# 회귀분석 결과에서 잔차만 추출
resid = mdl.fit().resid



# COMMAND ----------

from scipy.stats import probplot
plt.figure()
probplot(resid, plot=plt)
plt.show()
###...... 하..... 

# COMMAND ----------

from scipy import stats
stats.shapiro(resid)### p-value가 0.05보다 작으므로 귀무가설을 기각, 즉 데이터는 정규성을 만족한다고 볼 수 없습니다.

# COMMAND ----------

from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm

formula = 'p_matchTime ~ C(channelName)+C(teamPlayers)+C(channelName):C(teamPlayers)'
lm = ols(formula, df3).fit()
print(anova_lm(lm))

# COMMAND ----------

anova_lm(lm)

# COMMAND ----------

model = ols('p_matchTime ~ C(channelName)+C(teamPlayers)', data =df3).fit()

# COMMAND ----------

import statsmodels.api as sm
sm.stats.anova_lm(model, typ =2)

# COMMAND ----------

model = ols('p_matchTime ~ C(channelName)+C(teamPlayers)+C(channelName):C(teamPlayers)', data =df3).fit()
sm.stats.anova_lm(model, typ =2)

# COMMAND ----------

china2.registerTempTable("china2")

# COMMAND ----------

grandprix1 = spark.sql("select p_matchTime from china2 where channelName = 'grandprix_speedTeamInfinit' and teamPlayers = 1 ")
grandprix2 = spark.sql("select p_matchTime from china2 where channelName = 'grandprix_speedTeamInfinit' and teamPlayers = 2 ")
grandprix3 = spark.sql("select p_matchTime from china2 where channelName = 'grandprix_speedTeamInfinit' and teamPlayers = 3 ")
grandprix4 = spark.sql("select p_matchTime from china2 where channelName = 'grandprix_speedTeamInfinit' and teamPlayers = 4 ")

combine1 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamCombine' and teamPlayers = 1 ")
combine2 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamCombine' and teamPlayers = 2 ")
combine3 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamCombine' and teamPlayers = 3 ")
combine4 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamCombine' and teamPlayers = 4 ")

fast1 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFast' and teamPlayers = 1 ")
fast2 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFast' and teamPlayers = 2 ")
fast3 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFast' and teamPlayers = 3 ")
fast4 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFast' and teamPlayers = 4 ")

fastnewbie1 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastNewbie' and teamPlayers = 1 ")
fastnewbie2 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastNewbie' and teamPlayers = 2 ")
fastnewbie3 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastNewbie' and teamPlayers = 3 ")
fastnewbie4 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastNewbie' and teamPlayers = 4 ")

fastest1 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastest' and teamPlayers = 1 ")
fastest2 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastest' and teamPlayers = 2 ")
fastest3 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastest' and teamPlayers = 3 ")
fastest4 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamFastest' and teamPlayers = 4 ")

infinit1 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamInfinit' and teamPlayers = 1 ")
infinit2 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamInfinit' and teamPlayers = 2 ")
infinit3 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamInfinit' and teamPlayers = 3 ")
infinit4 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamInfinit' and teamPlayers = 4 ")

newbie1 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamNewbie' and teamPlayers = 1 ")
newbie2 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamNewbie' and teamPlayers = 2 ")
newbie3 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamNewbie' and teamPlayers = 3 ")
newbie4 = spark.sql("select p_matchTime from china2 where channelName = 'speedTeamNewbie' and teamPlayers = 4 ")

# tierMatching1 = spark.sql("select p_matchTime from china2 where channelName = 'tierMatching_speedTeam	' and teamPlayers = 1 ")
# tierMatching2 = spark.sql("select p_matchTime from china2 where channelName = 'tierMatching_speedTeam	' and teamPlayers = 2 ")
# tierMatching3 = spark.sql("select p_matchTime from china2 where channelName = 'tierMatching_speedTeam	' and teamPlayers = 3 ")
# tierMatching4 = spark.sql("select p_matchTime from china2 where channelName = 'tierMatching_speedTeam	' and teamPlayers = 4 ")


# COMMAND ----------

combine1.display()

# COMMAND ----------

grandprix1= grandprix1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
grandprix2= grandprix2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
grandprix3= grandprix3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
grandprix4= grandprix4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

combine1= combine1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
combine2= combine2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
combine3= combine3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
combine4= combine4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

fast1= fast1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fast2= fast2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fast3= fast3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fast4= fast4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

fastnewbie1= fastnewbie1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fastnewbie2= fastnewbie2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fastnewbie3= fastnewbie3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fastnewbie4= fastnewbie4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

fastest1= fastest1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fastest2= fastest2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fastest3= fastest3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
fastest4= fastest4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

infinit1= infinit1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
infinit2= infinit2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
infinit3= infinit3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
infinit4= infinit4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()

newbie1= newbie1.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
newbie2= newbie2.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
newbie3= newbie3.select('p_matchTime').rdd.flatMap(lambda x: x).collect()
newbie4= newbie4.select('p_matchTime').rdd.flatMap(lambda x: x).collect()


# COMMAND ----------

dd1 = pd.DataFrame({'treatment':'grandprix1', 'value':pd.Series(grandprix1)})
dd2 = pd.DataFrame({'treatment':'grandprix2', 'value':pd.Series(grandprix2)})
dd3 = pd.DataFrame({'treatment':'grandprix3', 'value':pd.Series(grandprix3)})
dd4 = pd.DataFrame({'treatment':'grandprix4', 'value':pd.Series(grandprix4)})

dd5 = pd.DataFrame({'treatment':'combine1', 'value':pd.Series(combine1)})
dd6 = pd.DataFrame({'treatment':'combine2', 'value':pd.Series(combine2)})
dd7 = pd.DataFrame({'treatment':'combine3', 'value':pd.Series(combine3)})
dd8 = pd.DataFrame({'treatment':'combine4', 'value':pd.Series(combine4)})


dd9 = pd.DataFrame({'treatment':'fast1', 'value':pd.Series(fast1)})
dd10 = pd.DataFrame({'treatment':'fast2', 'value':pd.Series(fast2)})
dd11 = pd.DataFrame({'treatment':'fast3', 'value':pd.Series(fast3)})
dd12 = pd.DataFrame({'treatment':'fast4', 'value':pd.Series(fast4)})


dd13 = pd.DataFrame({'treatment':'fastnewbie1', 'value':pd.Series(fastnewbie1)})
dd14 = pd.DataFrame({'treatment':'fastnewbie2', 'value':pd.Series(fastnewbie2)})
dd15 = pd.DataFrame({'treatment':'fastnewbie3', 'value':pd.Series(fastnewbie3)})
dd16 = pd.DataFrame({'treatment':'fastnewbie4', 'value':pd.Series(fastnewbie4)})

dd17 = pd.concat([dd1, dd2, dd3, dd4, dd5, dd6, dd7, dd8, dd9, dd10, dd11, dd12, dd13, dd14, dd15, dd16], axis=0)

# COMMAND ----------

dd17

# COMMAND ----------

dd18 = pd.DataFrame({'treatment':'fastest1', 'value':pd.Series(fastest1)})
dd19 = pd.DataFrame({'treatment':'fastest1', 'value':pd.Series(fastest1)})
dd20 = pd.DataFrame({'treatment':'fastest1', 'value':pd.Series(fastest1)})
dd21 = pd.DataFrame({'treatment':'fastest1', 'value':pd.Series(fastest1)})

dd22 = pd.DataFrame({'treatment':'infinit1', 'value':pd.Series(infinit1)})
dd23 = pd.DataFrame({'treatment':'infinit1', 'value':pd.Series(infinit1)})
dd24 = pd.DataFrame({'treatment':'infinit1', 'value':pd.Series(infinit1)})
dd25 = pd.DataFrame({'treatment':'infinit1', 'value':pd.Series(infinit1)})


dd26 = pd.DataFrame({'treatment':'newbie1', 'value':pd.Series(newbie1)})
dd27 = pd.DataFrame({'treatment':'newbie1', 'value':pd.Series(newbie1)})
dd28 = pd.DataFrame({'treatment':'newbie1', 'value':pd.Series(newbie1)})
dd29 = pd.DataFrame({'treatment':'newbie1', 'value':pd.Series(newbie1)})


# dd30 = pd.DataFrame({'treatment':'tierMatching4', 'value':pd.Series(tierMatching4)})
# dd31 = pd.DataFrame({'treatment':'tierMatching4', 'value':pd.Series(tierMatching4)})
# dd32 = pd.DataFrame({'treatment':'tierMatching4', 'value':pd.Series(tierMatching4)})
# dd33 = pd.DataFrame({'treatment':'tierMatching4', 'value':pd.Series(tierMatching4)})



# COMMAND ----------

dd34 = pd.concat([dd17, dd18, dd19, dd20, dd21, dd22, dd23, dd24, dd25, dd26, dd27, dd28, dd29], axis=0)
dd34

# COMMAND ----------

from statsmodels.stats.multicomp import pairwise_tukeyhsd
from statsmodels.stats.multicomp import MultiComparison

mc = MultiComparison(dd34['value'], dd34['treatment'])
mcresult = mc.tukeyhsd(0.05)
mcresult.summary()
### 열심히 노가다한건데..... 결과를 보니....... 휴 

# COMMAND ----------

# MAGIC %md
# MAGIC ### matchTime * playTime 상관분석

# COMMAND ----------

china7 = spark.sql("select channelName, p_matchTime, playTime, teamPlayers from august where track ='빌리지 손가락'")
df7 = china7.select("*").toPandas()

# COMMAND ----------

X = df7.playTime.values
Y = df7.p_matchTime.values

# COMMAND ----------

import matplotlib.pyplot as plt

plt.scatter(X, Y, alpha = 0.5)

plt.title('p_matchTime ~ playTime')

plt.xlabel('playTime')

plt.ylabel('p_matchTime')

plt.show() ######우와... 

# COMMAND ----------

### 공분산!!!일아 코이피션트!!!

#cov = (np.sum(X*Y) - len(X)*np.mean(X)*np.mean(Y))/len(X)
print(np.cov(X, Y)[0,1])
print(np.corrcoef(X,Y)[0,1])

# COMMAND ----------

import scipy.stats as stats
#### H0 :  matchtime과 playtime은 상관관계가 없다 
#### H1 : 상관관계가 있다 
stats.pearsonr(X, Y)      ####(뒤에 결과값 : p-value)
### 결과 해석 : 상관 관계가...... 괴이이이이앵장히 높습니다...... playtime을 ..... 빼야할까요? ㅜ

# COMMAND ----------

