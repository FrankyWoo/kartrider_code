# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD_labeled.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.count()

# COMMAND ----------

# 10 만개 실패 
tmp = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime from df order by random() limit 100000  ''')

# COMMAND ----------

#5 만개 실패 
tmp = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, label from df order by random() limit 50000  ''')

# COMMAND ----------

#3 만개 실패 
tmp = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, label from df order by random() limit 30000  ''')

# COMMAND ----------

#2 만개 실패
tmp = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, label from df order by random() limit 20000  ''')

# COMMAND ----------

tmp = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, label from df order by random() limit 10000  ''')

# COMMAND ----------

tmp.count()

# COMMAND ----------

tmp.display()

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = tmp.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("label",col("label").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))

print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()
panda.corr()

# COMMAND ----------

panda.shape

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

panda3 = panda.copy()

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio', 'playTime'] ### actual distance와 retired Ratio ..... scaling을 시켜주어야할까요?
panda3[numeric_data] = minscaler.fit(panda3[numeric_data]).transform(panda3[numeric_data])

# COMMAND ----------

feature = [x for x in panda3.columns if (x != 'p_accountNo') & (x != 'playTime') & (x!='p_matchTime') & (x!='label')]
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

#For demonstration ###별개
from statsmodels.formula.api import ols

#Model statistics
model1 = sm.OLS(panda3['p_matchTime'], sm.add_constant(panda3[feature])).fit()
print_model1 = model1.summary()
print(print_model1)

# COMMAND ----------

import statsmodels.formula.api as sm
result3 = sm.ols(formula = form, data = panda3 ).fit()
print(result3.summary())

# COMMAND ----------

import statsmodels.formula.api as sm
log_data2 = panda3
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 
result4 = sm.ols(formula = form, data = log_data2 ).fit()
print(result4.summary())

# COMMAND ----------

###backward 제거하면, 변수 많이 날라가는데.. 일단 그렇게 할까요?? => 이전에 큰 차이 없음 확인했기 때문에 보류하겠습니다 (우선은!)

# COMMAND ----------

log_data2.columns

# COMMAND ----------

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

# COMMAND ----------

influence = result4.get_influence()
pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------



# COMMAND ----------

C = influence.cooks_distance[0]

# COMMAND ----------

cutoff=(pmatchtimeres.loc[:,"cooks_d"].mean())*3
outlier2=pd.DataFrame((pmatchtimeres.p_matchTime[abs(pmatchtimeres.cooks_d) > cutoff]))
print(outlier2)

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

influence = result4.get_influence()
pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------

influence = result4.get_influence()
inf_sum = influence.summary_frame()

print(inf_sum.head())

# COMMAND ----------

influence = result4.get_influence()
inf_sum = influence.summary_frame()

print(inf_sum.head())

# COMMAND ----------

student_resid = influence.resid_studentized_external
(cooks, p) = influence.cooks_distance
(dffits, p) = influence.dffits
leverage = influence.hat_matrix_diag

# COMMAND ----------

print ('\n')
print ('Leverage vs. Studentized Residuals')
sns.regplot(leverage, result4.resid_pearson,  fit_reg=False)
plt.title('Leverage vs. Studentized Residuals')
plt.xlabel('Leverage')
plt.ylabel('Studentized Residuals')

# COMMAND ----------

#Concat MEDV and the resulting residual table
#Note that hat_diag is leverage so change the ciolumn heading from hat_diag to leverage
from statsmodels.formula.api import ols

pmatchtimeres = pd.concat([panda3.p_matchTime, inf_sum], axis = 1)
pmatchtimeres=pmatchtimeres.rename(columns={'hat_diag': 'leverage'})
pmatchtimeres.head()

# COMMAND ----------

#studentized residuals for identifying outliers
#requested studentized residuals call them r
#studentized residuals that exceed +2 or -2 are concerning
#studentized residuals that exceed +3 or -3 are extremely concerning

r = pmatchtimeres.student_resid
print ('-'*30 + ' studentized residual ' + '-'*30)
print (r.describe())
print ('\n')

r_sort = pmatchtimeres.sort_values(by = 'student_resid')
print ('-'*30 + ' top 5 most negative residuals ' + '-'*30)
print (r_sort.head())
print ('\n')

print ('-'*30 + ' top 5 most positive residuals ' + '-'*30)
print (r_sort.tail())

# COMMAND ----------

#Print all MEDV values where the studentized residuals exceed 2
print (pmatchtimeres.p_matchTime[abs(r) > 2])

# COMMAND ----------

#Identify high leverage
#point with leverage = (2k+2)/n 
#k = number of predictors (11)
#n = number of observations (506)
((2*11)+2)/506 #=0.04743083003952569 any numbner higher than this is high leverage
l = pmatchtimeres.leverage

print ('-'*30 + ' Leverage ' + '-'*30)
print (l.describe())
print ('\n')

l_sort = pmatchtimeres.sort_values(by = 'leverage', ascending = False)
print ('-'*30 + ' top 5 highest leverage data points ' + '-'*30)
print (l_sort.head())

# COMMAND ----------

leverage 

# COMMAND ----------

log_data3

# COMMAND ----------

#Identify high leverage
#point with leverage = (2k+2)/n 
#k = number of predictors (11)
#n = number of observations (506)
### 이 방법 맞나요...?leverage cutoff 계산... 
k = 7
n = 


# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------Cook's D ----------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

#Cook's D of more than 3 times the mean is a possible outlier
#MEDVres.loc[:,"cooks_d"].mean()
cutoff=(pmatchtimeres.loc[:,"cooks_d"].mean())*3
outlier2=pd.DataFrame((pmatchtimeres.p_matchTime[abs(pmatchtimeres.cooks_d) > cutoff]))
print(outlier2)

# COMMAND ----------

pmatchtimeres.loc[:,"cooks_d"]

# COMMAND ----------

C = influence.cooks_distance[0]
C

# COMMAND ----------

outlier2

# COMMAND ----------

pd.concat([log_data2, outlier2], ignore_index=True, sort=False)

# COMMAND ----------

plz = pd.merge(log_data2,outlier2, left_index=True, right_index=True)
plz

# COMMAND ----------

pmatchtimeres["result"] = ""
pmatchtimeres

# COMMAND ----------

panda3.iloc[33]

# COMMAND ----------

pmatchtimeres.iloc[33]

# COMMAND ----------

# MAGIC %md
# MAGIC 통과 개수 : 

# COMMAND ----------

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

# COMMAND ----------

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

influence = OLSInfluence(result4)
sresiduals = influence.resid_studentized_internal
print(sresiduals.idxmin(), sresiduals.min())

outlier = panda.loc[sresiduals.idxmin(), :]
print('p_matchTime', outlier[target])
print(outlier[predictors])

# COMMAND ----------

pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------

#influence = OLSInfluence(result4)
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

C = influence.cooks_distance[0]

# COMMAND ----------

C

# COMMAND ----------

influence.cooks_distance