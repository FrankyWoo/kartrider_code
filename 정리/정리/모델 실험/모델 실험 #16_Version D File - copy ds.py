# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD.csv"))

df.registerTempTable("df")

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

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

# COMMAND ----------

del panda['playTime']

# COMMAND ----------

import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

panda.columns

# COMMAND ----------

# MAGIC %md
# MAGIC scaling - feature values 

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
#scaler.fit(numeric_data)
numeric_data = ['actualdistance', 'retiredRatio']
panda[numeric_data] = scaler.fit(panda[numeric_data]).transform(panda[numeric_data])

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC Label Encoding

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

# MAGIC %md
# MAGIC get_dummies (drop first row )

# COMMAND ----------

data = pd.get_dummies(panda, columns=cat_columns, drop_first=True)
data

# COMMAND ----------

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
print(result.summary())

# COMMAND ----------

influence = OLSInfluence(result)
sresiduals = influence.resid_studentized_internal

print(sresiduals.idxmin(), sresiduals.min())

# COMMAND ----------

predictors =  X_df.reindex(columns=predictors)

# COMMAND ----------

outlier