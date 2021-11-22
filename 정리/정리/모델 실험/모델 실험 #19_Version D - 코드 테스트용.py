# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD.csv"))

df.registerTempTable("df")

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

### 제거하지 않겠습니다. del panda['playTime']

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio', 'playTime']
panda[numeric_data] = minscaler.fit(panda[numeric_data]).transform(panda[numeric_data])

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

feature = [x for x in panda.columns if x != 'p_matchTime']
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

# COMMAND ----------

form 

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+actualdistance+retiredRatio+playTime'

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = panda ).fit()
print(result.summary())

# COMMAND ----------

import numpy as np
### 비선형일 것을 염려해서................... log로 선형 만들었습니다. 
panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 
result = sm.ols(formula = form, data = panda ).fit()
print(result.summary())

# COMMAND ----------

#### 사실 어떤 모델이 맞는 지 모르겠습니다! 맞는 모델 최종 확인되면 backward eliminmation 하겠습니다. 지금까지 해본 결과, 사실.... 저희 데이터의 경우,,,,,,,, R^2 값이나 AIC에 아무런 영햐잉 없어 그냥 진행하겠습니다. 

# COMMAND ----------

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio','playTime']
target = 'p_matchTime'

influence = OLSInfluence(result)
sresiduals = influence.resid_studentized_internal
print(sresiduals.idxmin(), sresiduals.min())

outlier = panda.loc[sresiduals.idxmin(), :]
print('p_matchTime', outlier[target])
print(outlier[predictors])

# COMMAND ----------

pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------

influence = OLSInfluence(result)
C = influence.cooks_distance[0]

Cook = pd.DataFrame(columns = ['Id', "Cook_'s Distance"])
Cook['Id'] = panda.p_accountNo
Cook["Cook's Distance"] = C
fig = px.line(Cook, x="Id", y="Cook's Distance", title = "Cook's Distance")
fig.show()

# COMMAND ----------

data = pd.DataFrame(columns = ['Id', 'X', 'Y','Dist'])
data['Id'] = panda.p_accountNo
data['X'] = influence.hat_matrix_diag
data['Y'] = influence.resid_studentized_internal
data['Dist'] = influence.cooks_distance[0]
fig = px.scatter(data, x='X', y='Y', hover_data = ["Id"])
fig.show()

# COMMAND ----------

import plotly.graph_objs as go
from plotly.offline import iplot, init_notebook_mode
import cufflinks

cufflinks.go_offline(connected=True)
init_notebook_mode(connected=True)
import pandas as pd

data.iplot(
    x='X',
    y='Y',
    size=data['Dist']*10, 
    text='Dist',
    mode='markers',
    layout=dict(
        xaxis=dict(title='Id'),
        yaxis=dict(title='Studentized residuals'),
        title="Id vs Studentized residuals sized by Cook's Distance"))


# COMMAND ----------

influence = OLSInfluence(result)

# cooks_distance is an attribute of incluence, here C, not sure about P (p-value maybe?)
C = influence.cooks_distance[0]

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 8))
plt.plot(C)
plt.xlabel('Id')
plt.ylabel("Cook's Distance")

# COMMAND ----------

###첫번째 참고 자료 : https://mdsohelmahmood.github.io/2021/07/21/Cook's-Distance.html#Outlier-analysis
### : https://towardsdatascience.com/outlier-detection-in-regression-using-cooks-distance-f5e4954461a0
### 두번째 참고 자료 : https://www.datasklr.com/ols-least-squares-regression/diagnostics-for-leverage-and-influence

# COMMAND ----------

import pandas as pd
import numpy as np
import itertools
from itertools import chain, combinations
import statsmodels.formula.api as sm
import scipy.stats as scipystats
import statsmodels.api as sm
import statsmodels.stats.stattools as stools
import statsmodels.stats as stats 
from statsmodels.graphics.regressionplots import *
import matplotlib.pyplot as plt
import seaborn as sns
import copy
from sklearn.model_selection import train_test_split
import math
import time

# COMMAND ----------

import pandas as pd
from sklearn.datasets import load_boston
boston= load_boston()

# COMMAND ----------

#Create analytical data set
#Create dataframe from feature_names
boston_features_df = pd.DataFrame(data=boston.data,columns=boston.feature_names)

#Create dataframe from target
boston_target_df = pd.DataFrame(data=boston.target,columns=['MEDV'])

# COMMAND ----------

boston_features_df

# COMMAND ----------

boston_target_df

# COMMAND ----------

influence = result.get_influence()
pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------

influence = result.get_influence()
inf_sum = influence.summary_frame()

print(inf_sum.head())

# COMMAND ----------

student_resid = influence.resid_studentized_external
(cooks, p) = influence.cooks_distance
(dffits, p) = influence.dffits
leverage = influence.hat_matrix_diag


print ('\n')
print ('Leverage vs. Studentized Residuals')
sns.regplot(leverage, result.resid_pearson,  fit_reg=False)
plt.title('Leverage vs. Studentized Residuals')
plt.xlabel('Leverage')
plt.ylabel('Studentized Residuals')


# COMMAND ----------

#Concat MEDV and the resulting residual table
#Note that hat_diag is leverage so change the ciolumn heading from hat_diag to leverage
from statsmodels.formula.api import ols

MEDVres = pd.concat([panda.p_matchTime, inf_sum], axis = 1)
MEDVres=MEDVres.rename(columns={'hat_diag': 'leverage'})
MEDVres.head()

# COMMAND ----------


r = MEDVres.student_resid
print ('-'*30 + ' studentized residual ' + '-'*30)
print (r.describe())
print ('\n')

r_sort = MEDVres.sort_values(by = 'student_resid')
print ('-'*30 + ' top 5 most negative residuals ' + '-'*30)
print (r_sort.head())
print ('\n')

print ('-'*30 + ' top 5 most positive residuals ' + '-'*30)
print (r_sort.tail())

# COMMAND ----------

#Print all MEDV values where the studentized residuals exceed 2
print (MEDVres.p_matchTime[abs(r) > 2])

# COMMAND ----------

#### 요고 참고 : https://www.machinelearningplus.com/linear-regression-in-julia/