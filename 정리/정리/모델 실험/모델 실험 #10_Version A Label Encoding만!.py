# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))

df.registerTempTable("df")

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("p_matchRetired",col("p_matchRetired").cast(StringType()))\
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))


print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()

# COMMAND ----------

del panda['rs']
del panda['trackId']
import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

num_cols = ['playTime', 'actualdistance', 'retiredRatio' ]

# COMMAND ----------

numeric_data = panda[num_cols].values
numeric_data

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaler.fit(numeric_data)

# COMMAND ----------

print(scaler.mean_)

# COMMAND ----------

panda[num_cols] = scaler.transform(panda[num_cols])

# COMMAND ----------

num_cols

# COMMAND ----------

import numpy as np
np.log1p(panda['p_matchTime']) 

# COMMAND ----------

panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 
panda['p_matchTime']

# COMMAND ----------

import mlflow.sklearn
mlflow.sklearn.autolog()

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

del panda['playTime']

# COMMAND ----------

panda

# COMMAND ----------

# MAGIC %md 
# MAGIC get dummies

# COMMAND ----------

import pandas as pd
pd.concat((panda,pd.get_dummies(panda, columns=cat_columns, drop_first=True)), axis=1) #model에 drop 여부 확인

# COMMAND ----------

cat_columns

# COMMAND ----------

cat = ['channelName', 'p_rankinggrade2', 'teamPlayers', 'difficulty', 'p_matchRetired', 'gameSpeed']

# COMMAND ----------

data = pd.get_dummies(panda, columns=cat, drop_first=True)
data

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

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.params)

# COMMAND ----------

print(result.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC Feature Selection

# COMMAND ----------

form

# COMMAND ----------

form2 = 'p_matchTime ~actualdistance+track+retiredRatio+channelName_1+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+teamPlayers_1+teamPlayers_2+teamPlayers_3+difficulty_1+difficulty_2+difficulty_3+difficulty_4+difficulty_5+p_matchRetired_1+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4'

# COMMAND ----------

result2 = sm.ols(formula = form2, data = data ).fit()
print(result2.params)

# COMMAND ----------

print(result2.summary())

# COMMAND ----------

form3 = 'p_matchTime ~actualdistance+track+retiredRatio+channelName_1+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+teamPlayers_1+teamPlayers_2+teamPlayers_3+difficulty_1+difficulty_2+difficulty_3+difficulty_4+difficulty_5+p_matchRetired_1+gameSpeed_1+gameSpeed_2+gameSpeed_3'

# COMMAND ----------

result3 = sm.ols(formula = form3, data = data ).fit()
result3.summary()

# COMMAND ----------

print(result3.summary())

# COMMAND ----------

form4 = 'p_matchTime ~actualdistance+track+retiredRatio+channelName_1+channelName_3+channelName_4+channelName_5+channelName_6+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+teamPlayers_1+teamPlayers_2+teamPlayers_3+difficulty_1+difficulty_2+difficulty_3+difficulty_4+difficulty_5+p_matchRetired_1+gameSpeed_1+gameSpeed_2+gameSpeed_3'

# COMMAND ----------

result4 = sm.ols(formula = form4, data = data ).fit()
print(result4.summary())

# COMMAND ----------

form5 = 'p_matchTime ~actualdistance+track+retiredRatio+channelName_1+channelName_3+channelName_4+channelName_5+channelName_6+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+teamPlayers_1+teamPlayers_2+teamPlayers_3+difficulty_1+difficulty_2+difficulty_3+difficulty_4+difficulty_5+gameSpeed_1+gameSpeed_2+gameSpeed_3'

# COMMAND ----------

result5 = sm.ols(formula = form5, data = data ).fit()
print(result5.summary())