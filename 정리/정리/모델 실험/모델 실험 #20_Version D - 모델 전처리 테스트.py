# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD.csv"))

df.registerTempTable("df")

# COMMAND ----------

df2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD_labeled.csv"))

df2.registerTempTable("df2")

# COMMAND ----------

df2.count()

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/5maps2.csv")

# COMMAND ----------



# COMMAND ----------

df.count()

# COMMAND ----------

df = spark.sql("select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime from df limit 5000")
df.display()

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))

print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()

# COMMAND ----------

panda2 = df2.toPandas()

# COMMAND ----------

panda3 = df2.toPandas()

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

import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

panda

# COMMAND ----------

panda2

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio', 'playTime'] ### actual distance와 retired Ratio ..... scaling을 시켜주어야할까요?
panda[numeric_data] = minscaler.fit(panda[numeric_data]).transform(panda[numeric_data])

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['playTime'] ### actual distance와 retired Ratio ..... scaling을 시켜주어야할까요?
panda2[numeric_data] = minscaler.fit(panda2[numeric_data]).transform(panda2[numeric_data])

# COMMAND ----------

# from sklearn.preprocessing import LabelEncoder
# le = LabelEncoder()
# panda['channelName']  = le.fit_transform(panda['channelName'])


# COMMAND ----------

# panda

# COMMAND ----------

panda['channelName'].value_counts()

# COMMAND ----------

# data = pd.get_dummies(panda, columns=['channelName'], drop_first=True)
# data

# COMMAND ----------

data = pd.get_dummies(panda, columns=['channelName'])
data

# COMMAND ----------

data3 = pd.get_dummies(panda2, columns=['channelName'])
data3

# COMMAND ----------

data4 = pd.get_dummies(panda2, columns=['channelName'])
data4

# COMMAND ----------

data7 = pd.get_dummies(panda3, columns=['channelName'])
data7

# COMMAND ----------

### ordinal encoding => 할 필요가 없다 판단했습니다 
# from category_encoders import OrdinalEncoder

# enc1 = OrdinalEncoder(cols = 'color')
# df2 = enc1.fit_transform(df2)
# df2

# COMMAND ----------

feature = [x for x in data.columns if x != 'p_matchTime']
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

form =   'p_matchTime ~gameSpeed+teamPlayers+p_rankinggrade2+difficulty+actualdistance+retiredRatio+playTime+channelName_speedTeamCombine+channelName_speedTeamFastest+channelName_speedTeamInfinit+channelName_speedTeamNewbie'

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

import numpy as np
### 비선형일 것을 염려해서................... log로 선형 만들었습니다. 
data2 = data
data2['p_matchTime'] = np.log1p(data2['p_matchTime']) 
result2 = sm.ols(formula = form, data = data2 ).fit()
print(result2.summary())

# COMMAND ----------

###R^2 value가... 너무 높게 나와서 문제이긴 한데..... 그렇다하더라도..... 첫번째가..... 더빈왓슨값이 타당하지 않을까요...?

# COMMAND ----------

###### Min max scaling 안한거 
#data2 = data3
#data2['p_matchTime'] = np.log1p(data2['p_matchTime']) 
result3 = sm.ols(formula = form, data = data7 ).fit()
print(result3.summary())

# COMMAND ----------

###### Min max scaling 안한거 
data2 = data3
data2['p_matchTime'] = np.log1p(data2['p_matchTime']) 
result4 = sm.ols(formula = form, data = data2 ).fit()
print(result3.summary())

# COMMAND ----------

print(result4.summary())

# COMMAND ----------

###### Min max scaling 은 playtime만
data2 = data4
#data2['p_matchTime'] = np.log1p(data2['p_matchTime']) 
result5 = sm.ols(formula = form, data = data4 ).fit()
print(result5.summary())

# COMMAND ----------

###### Min max scaling 은 playtime만
data2 = data4
data2['p_matchTime'] = np.log1p(data2['p_matchTime']) 
result6 = sm.ols(formula = form, data = data2 ).fit()
print(result6.summary())

# COMMAND ----------

###우선 모델은 result4이용할게요
result4.summary()

# COMMAND ----------

from statsmodels.stats.outliers_influence import OLSInfluence
cd, _ = OLSInfluence(result4).cooks_distance
cd.sort_values(ascending=False).head()

# COMMAND ----------

cd.sort_values(ascending=True)

# COMMAND ----------

cd.sort_values(ascending=False)

# COMMAND ----------

#calculate studentized residuals
stud_res = result4.outlier_test()

#display studentized residuals
print(stud_res)

# COMMAND ----------

import matplotlib.pyplot as plt

#define predictor variable values and studentized residuals
x = df['points']
y = stud_res['student_resid']

#create scatterplot of predictor variable vs. studentized residuals
plt.scatter(x, y)
plt.axhline(y=0, color='black', linestyle='--')
plt.xlabel('Points')
plt.ylabel('Studentized Residuals') 

# COMMAND ----------

influence = result4.get_influence()
student_resid = influence.resid_studentized_external
(cooks, p) = influence.cooks_distance
(dffits, p) = influence.dffits
leverage = influence.hat_matrix_diag

# COMMAND ----------

student_resid

# COMMAND ----------

inf_sum = influence.summary_frame()

# COMMAND ----------

inf_sum