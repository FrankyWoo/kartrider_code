# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))

df.registerTempTable("df")

# COMMAND ----------

df_answer=spark.sql("select * from df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실','코리아 롯데월드 어드벤처')")

sample_pd=df_answer.toPandas()

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

# sample_lotte=sample_pd[sample_pd["track"]=="코리아 롯데월드 어드벤처"]
# sample_lotte["label"] = [0 if  181>= x >=122 else 1 for x in sample_lotte["p_matchTime"]]

# COMMAND ----------

concated=pd.concat([sample_brody,sample_china_rush,sample_china_dragon,sample_villeage,sample_basement])

# COMMAND ----------

test = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA_labeled.csv"))

test.registerTempTable("test") 

# COMMAND ----------

test.display()

# COMMAND ----------

test.count()

# COMMAND ----------

test.groupBy('track').count().orderBy('count').show()

# COMMAND ----------

#versionA_labeled.csv

# COMMAND ----------

answer = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD_labeled.csv"))

answer.registerTempTable("answer")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

df = df.withColumn('id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)

# COMMAND ----------

df.registerTempTable("df")
df.display()

# COMMAND ----------

df.groupBy('id').count().orderBy('count').show()

# COMMAND ----------

df.count()

# COMMAND ----------

tmp = spark.sql('''select id,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, track from df  ''')

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = tmp.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("id",col("id").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))

print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()
panda.corr() ###playTime 변수로 안넣음. 

# COMMAND ----------

panda

# COMMAND ----------

from pathlib import Path
 
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.linear_model import LinearRegression
 
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.stats.outliers_influence import OLSInfluence
 
from pygam import LinearGAM, s, l
from pygam.datasets import wage
 
import seaborn as sns
import matplotlib.pyplot as plt
 
import plotly.express as px

# COMMAND ----------

panda.columns

# COMMAND ----------

panda3 = panda.copy()

# COMMAND ----------

panda3 = panda.copy()
from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio', 'playTime'] ### actual distance와 retired Ratio ..... scaling을 시켜주어야할까요?
panda3[numeric_data] = minscaler.fit(panda3[numeric_data]).transform(panda3[numeric_data])
# feature = [x for x in panda3.columns if (x != 'p_accountNo') & (x != 'playTime') & (x!='p_matchTime') & (x!='label')]
# val = ""
# cnt = 0 
# for i in feature:
#   if cnt != 0:
#     val +=  "+" + i
#     cnt += 1
#   else:
#     val += i
#     cnt += 1
# form = "p_matchTime ~" + val
# form

# COMMAND ----------

numeric_data

# COMMAND ----------

feature = [x for x in panda3.columns if (x != 'p_accountNo') & (x != 'playTime') & (x!='p_matchTime') & (x!='id')]
val = ""
cnt = 0 
for i in feature:
  if cnt != 0:
    val +=  "+" + i
    cnt += 1
  else:
    val += i
    cnt += 1
form = "p_matchTime ~" + val
form

# COMMAND ----------

import statsmodels.formula.api as sm
log_data2 = panda3
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 

# COMMAND ----------

###log 안씌우기

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+actualdistance+retiredRatio+track'

# COMMAND ----------

import statsmodels.formula.api as sm
### Track Id 포함 
model = sm.ols(formula = form, data = panda).fit()
print(model.summary())

# COMMAND ----------

model.summary()

# COMMAND ----------

#scaling
model2 = sm.ols(formula = form, data = panda3).fit()
print(model2.summary())

# COMMAND ----------

model2.summary()

# COMMAND ----------

### Track Id 포함
import statsmodels.formula.api as sm
log_data2 = panda3
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 
result = sm.ols(formula = form, data = log_data2 ).fit()
print(result.summary()) ## durbin-watson : 1.5 ~ 2.5 사이 

# COMMAND ----------

result.summary()

# COMMAND ----------

### actualdistance와 retiredRatio 제거해도 된다 판단했습니다. 
form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+track'

# COMMAND ----------

result2 = sm.ols(formula = form, data = log_data2 ).fit()
print(result2.summary())

# COMMAND ----------

result2.summary()

# COMMAND ----------

###  difficulty 제거해도 된다 판단했습니다. 
form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+track'

# COMMAND ----------

import statsmodels.formula.api as sm
log_data2 = panda3
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 
result3 = sm.ols(formula = form, data = log_data2 ).fit()
print(result3.summary())

# COMMAND ----------

result3 = sm.ols(formula = form, data = log_data2 ).fit()
print(result3.summary())

# COMMAND ----------

result3.summary()

# COMMAND ----------

panda['teamPlayers'].value_counts()

# COMMAND ----------

panda['channelName'].value_counts()

# COMMAND ----------

panda['gameSpeed'].value_counts()

# COMMAND ----------

panda['difficulty'].value_counts()

# COMMAND ----------

panda

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+actualdistance+retiredRatio'

# COMMAND ----------

result4 = sm.ols(formula = form, data = log_data2 ).fit()
print(result4.summary())

# COMMAND ----------

result4.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC =======================================================================================================================================================

# COMMAND ----------

### SparkML 이용 할까요? 우선 참고 사항을 위해 가장 괜찮다 생각한 result3 모델에 대해 ML 모델 사용해보겠습니다. 코드가 동일하니 이후 필요 사항에 의해 변수만 변경하면 됩니다. 

# COMMAND ----------

### Cook's Distance 
from statsmodels.stats.outliers_influence import OLSInfluence
cd, _ = OLSInfluence(result3).cooks_distance
cd.sort_values(ascending=False).head()

# COMMAND ----------

log_data2.index

# COMMAND ----------

print(log_data2.index)

# COMMAND ----------

log_data2

# COMMAND ----------

type(cd)

# COMMAND ----------

cd[927180]

# COMMAND ----------

tmp = cd.to_frame()
tmp.rename(columns = {0: "outcome"}, inplace = True)

# COMMAND ----------

tmp

# COMMAND ----------

cd.sort_values(ascending=False).head(10)

# COMMAND ----------

tmp["cook_s"].mean()

# COMMAND ----------

tmp.loc[:,"cook_s"].mean()

# COMMAND ----------

#### 참고 사이트 : https://towardsdatascience.com/identifying-outliers-in-linear-regression-cooks-distance-9e212e9136a
cutoff=(tmp.loc[:,"cook_s"].mean())*3
cutoff

# COMMAND ----------

cooks_outlier = pd.DataFrame(tmp.cook_s > cutoff)

# COMMAND ----------

cooks_outlier

# COMMAND ----------

cooks_outlier2 = pd.DataFrame(tmp.cook_s > 0.5)

# COMMAND ----------

cutoff2=(tmp.loc[:,"cook_s"].mean())*4
print("cutoff : ", cutoff2)
cooks_outlier3 = pd.DataFrame(tmp.cook_s > cutoff2)

# COMMAND ----------

cooks_outlier3[cooks_outlier3['cook_s'] == True]

# COMMAND ----------

cooks_outlier2[cooks_outlier2['cook_s'] == True]

# COMMAND ----------

cooks_outlier[cooks_outlier['cook_s'] == True]

# COMMAND ----------

cooks_outlier.loc[927180,:]

# COMMAND ----------

log_data2['outcome'] = ""

# COMMAND ----------

new = pd.concat([log_data2, tmp], axis = 1)

# COMMAND ----------

cd

# COMMAND ----------

del new['cook_s']

# COMMAND ----------

new

# COMMAND ----------

cd

# COMMAND ----------

new = pd.concat([new, tmp], axis = 1)
new = pd.concat([new, cooks_outlier], axis = 1)
new

# COMMAND ----------

len(new[new['cook_s']==True])

# COMMAND ----------

idx = new.index[new['cook_s']==True].tolist()
idx

# COMMAND ----------

a = ""
for i in idx:
  a += " "
  a += str(i)

# COMMAND ----------

a

# COMMAND ----------



# COMMAND ----------

a = ""
for i in idx:
  a += " "
  a += str(i)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

answer.display()

# COMMAND ----------

tmp2 = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, label, track from answer  ''')

# COMMAND ----------

answerpanda = tmp2.toPandas()

# COMMAND ----------

answerpanda

# COMMAND ----------

answerpanda['track'].value_counts()

# COMMAND ----------

rslt_df = dataframe[dataframe['Percentage'] > 80]

# COMMAND ----------

log_data2_answer = new[new['track'] == '빌리지 운명의 다리']

# COMMAND ----------

log_data2일찍 보

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

influence = result3.get_influence()
hat = influence.hat_matrix_diag

# COMMAND ----------



# COMMAND ----------

hat

# COMMAND ----------

new

# COMMAND ----------

##new 랑 hat 값 조인 


# COMMAND ----------

### residual
new["e"] = ""


# COMMAND ----------

leverage = influence.hat_matrix_diag

# COMMAND ----------

print(leverage)

# COMMAND ----------

len(leverage)

# COMMAND ----------

lev = pd.DataFrame(leverage)
lev

# COMMAND ----------

lev.rename(columns = {0: "leverage"}, inplace = True)

# COMMAND ----------

lev

# COMMAND ----------

#### Linearity Test

# COMMAND ----------

panda

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

influence = result3.get_influence()

# COMMAND ----------

(cooks, p) = influence.cooks_distance

# COMMAND ----------

cooks

# COMMAND ----------

(dffits, p) = influence.dffits

# COMMAND ----------

dffits