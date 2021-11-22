# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD.csv"))

df.registerTempTable("df")

# COMMAND ----------

df = spark.sql("select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime from df")
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

from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio']
panda[numeric_data] = minscaler.fit(panda[numeric_data]).transform(panda[numeric_data])

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

del panda['playTime']

# COMMAND ----------

panda

# COMMAND ----------

type(panda)

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

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = panda ).fit()
print(result.summary())

# COMMAND ----------

import numpy as np
panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 
result = sm.ols(formula = form, data = panda ).fit()
print(result.summary())

# COMMAND ----------

# import numpy as np
# import statsmodels.formula.api as sm
# panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 
# result = sm.ols(formula = form, data = panda ).fit()
# print(result.summary())

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+actualdistance+retiredRatio'

# COMMAND ----------

result = sm.ols(formula = form, data = panda ).fit()
print(result.summary())

# COMMAND ----------

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

# COMMAND ----------


influence = OLSInfluence(result)
sresiduals = influence.resid_studentized_internal
print(sresiduals.idxmin(), sresiduals.min())

outlier = panda.loc[sresiduals.idxmin(), :]
print('p_matchTime', outlier[target])
print(outlier[predictors])

# COMMAND ----------

pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------

model1 = result
model1

# COMMAND ----------

model1

# COMMAND ----------

influence = model1.get_influence()
inf_sum = influence.summary_frame()

print(inf_sum.head())


student_resid = influence.resid_studentized_external
(cooks, p) = influence.cooks_distance
(dffits, p) = influence.dffits
leverage = influence.hat_matrix_diag


print ('\n')
print ('Leverage vs. Studentized Residuals')
sns.regplot(leverage, model1.resid_pearson,  fit_reg=False)
plt.title('Leverage vs. Studentized Residuals')
plt.xlabel('Leverage')
plt.ylabel('Studentized Residuals')

#Concat MEDV and the resulting residual table
#Note that hat_diag is leverage so change the ciolumn heading from hat_diag to leverage
from statsmodels.formula.api import ols

MEDVres = pd.concat([panda.p_matchTime, inf_sum], axis = 1)
MEDVres=MEDVres.rename(columns={'hat_diag': 'leverage'})
MEDVres.head()

# COMMAND ----------

panda

# COMMAND ----------

influence = result.get_influence()
inf_sum = influence.summary_frame()

print(inf_sum.head())

# COMMAND ----------

influence = result.get_influence()
inf_sum = influence.summary_frame()

# COMMAND ----------

influence = model1.get_influence()

# COMMAND ----------

student_resid = influence.resid_studentized_external
print
(cooks, p) = influence.cooks_distance

(dffits, p) = influence.dffits
leverage = influence.hat_matrix_diag

# COMMAND ----------

print ('\n')
print ('Leverage vs. Studentized Residuals')
sns.regplot(leverage, model1.resid_pearson,  fit_reg=False)
plt.title('Leverage vs. Studentized Residuals')
plt.xlabel('Leverage')
plt.ylabel('Studentized Residuals')

# COMMAND ----------

#Concat MEDV and the resulting residual table
#Note that hat_diag is leverage so change the ciolumn heading from hat_diag to leverage
from statsmodels.formula.api import ols

MEDVres = pd.concat([boston_df.MEDV, inf_sum], axis = 1)
MEDVres=MEDVres.rename(columns={'hat_diag': 'leverage'})
MEDVres.head()

# COMMAND ----------

panda3 = panda[:100]

# COMMAND ----------

influence = OLSInfluence(result)
C = influence.cooks_distance[0]

# COMMAND ----------

C