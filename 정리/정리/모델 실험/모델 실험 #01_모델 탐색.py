# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD_labeled.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRank, count(p_matchRank) from df where p_matchRank = 1 group by p_matchRank

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from df where p_matchRank != 1 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRank, count(*) from df where p_matchRank != 1 group by p_matchRank

# COMMAND ----------

df.count()

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

panda.corr()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 이번 모델의 경우, get_dummies에서 첫 번째 category 유지 하지 않습니다........ 다중공선성 위협 문제로 인해서요! ( drop_first=False)
# MAGIC #### playTime을 우선 independent variable로 추가했습니다. 다중공선성 issue는 independent variable간의 관계도가 높을 경우 발생하기 때문에 문제가 되지 않지만, 1등을 한 player들의 기록이 playTime과 일치한다는 점이 고민이 되었습니다. 약 1/3 player들의 playTime과 matchTime이 같다는 점으로 인해 playTime을 우선 추가하지 않았습니다. 

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

# MAGIC %md
# MAGIC ### Model 01 : No scaling

# COMMAND ----------

data = pd.get_dummies(panda, columns=['channelName'])
data

# COMMAND ----------

feature = [x for x in data.columns if (x != 'p_accountNo') & (x != 'playTime')]
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
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

feature = [x for x in panda.columns if (x != 'p_accountNo') & (x != 'playTime') & (x!= 'p_matchTime')]
val = ""
cnt = 0 
for i in feature:
  if cnt != 0:
    val +=  "+" + i
    cnt += 1
  else:
    val += i
    cnt += 1
form2 = "p_matchTime ~" + val
form2

# COMMAND ----------

result = sm.ols(formula = form2, data = panda ).fit()
print(result.summary())

# COMMAND ----------

log_data = panda
log_data['p_matchTime'] = np.log1p(log_data['p_matchTime']) 
result2 = sm.ols(formula = form2, data = log_data ).fit()
print(result2.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model 1, 2 정리 :
# MAGIC R값이... 얼토당토 안해서 ........ 휴...... </br>
# MAGIC 더빈 왓슨 값도.... 생각보다 낮습니다.. </br>
# MAGIC 선형 유무 고민했는데... R^2 값이 저렇게 높은 경우는 고민할 필요도 없겠죠?ㅠ
# MAGIC 
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model 3, 4 : Min Max scaling 적용 => actual distance, retired ratio

# COMMAND ----------

panda3 = panda.copy()

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio', 'playTime'] ### actual distance와 retired Ratio ..... scaling을 시켜주어야할까요?
panda3[numeric_data] = minscaler.fit(panda3[numeric_data]).transform(panda3[numeric_data])

# COMMAND ----------

panda

# COMMAND ----------

panda3

# COMMAND ----------


feature = [x for x in panda3.columns if (x != 'p_accountNo') & (x != 'playTime') & (x!='p_matchTime') ]
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

result3 = sm.ols(formula = form, data = panda3 ).fit()
print(result3.summary())

# COMMAND ----------

log_data2 = panda3
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 
result4 = sm.ols(formula = form, data = log_data2 ).fit()
print(result4.summary())


# COMMAND ----------

# MAGIC %md
# MAGIC 결론 :비록 R^2 값이 적긴 하지만, 더빈 왓슨 값은 ok입니다. 우선.... 이 모델 채택해서...... cook's distance 하겠습니다. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 모델들 R^2 값이 말이 너무.... 안되는 관계로....... Ordinal Encoding을 channelName을 제외한 변수들에 적용시켜보겠습니다. 
# MAGIC 사실..... 저렇게 구분을 잘 해서.. 할 필요가 없다 생각했는데... 

# COMMAND ----------

panda

# COMMAND ----------

panda

# COMMAND ----------

### ordinal encoding => 할 필요가 없다 판단했습니다 ==> 순서가... 너무 엉망이긴 합니다.. 
from category_encoders import OrdinalEncoder
ord_col = ['gameSpeed', 'teamPlayers', 'p_rankinggrade2','difficulty']
enc1 = OrdinalEncoder(cols = ord_col)
panda5 = enc1.fit_transform(panda)
panda5

# COMMAND ----------

# MAGIC %md
# MAGIC ordinalencoder 순서가 너무 엉망인관계로.. 다른걸 이용해보겠습니다

# COMMAND ----------

panda6 = panda.copy()
panda6.head()

# COMMAND ----------

panda6

# COMMAND ----------

result5 = sm.ols(formula = form, data = panda6 ).fit()
print(result5.summary())

# COMMAND ----------

# MAGIC %md 
# MAGIC 결과가 말이 안되는 관계로...... 제 생각엔.. 대부분이 범주형이어서 그런것 같습니다. 우선, 모델 퍼레미터나 스케일링 방법은.. 죄송하지만 멘토님과 더 얘기해보고 미팅때 확정짓고 싶어요 ㅠ 모델을 GLM으로 해볼까요...? 근데... 카테고리가 많아서.. 
# MAGIC 카테고리... 경우 이용하는 방법을 써야하지 않나 싶어요 ㅠ우선 get_dummies 이용해보겠습니다. 

# COMMAND ----------

panda 

# COMMAND ----------

plz = pd.get_dummies(panda, columns=['channelName'], drop_first=True )
plz

# COMMAND ----------

feature = [x for x in plz.columns if (x != 'p_accountNo') & (x != 'playTime') & (x != 'p_matchTime')]
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
result6 = sm.ols(formula = form, data = plz ).fit()
print(result6.summary())

# COMMAND ----------

### Cook's distance

# COMMAND ----------

from statsmodels.stats.outliers_influence import OLSInfluence
cd, _ = OLSInfluence(result4).cooks_distance
cd.sort_values(ascending=False).head()

# COMMAND ----------

cd.sort_values(ascending=False)