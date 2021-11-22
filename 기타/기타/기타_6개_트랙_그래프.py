# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionC.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select track, min(p_matchTime), max(p_matchTime), mean(p_matchTime) from df group by track

# COMMAND ----------

### validation 가능할지 확인
tmp = spark.sql("select track, collect_list(p_matchTime) as matchtime from df group by track")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, p_matchTime from df where track = '차이나 골목길 대질주'

# COMMAND ----------

a = spark.sql("select track, p_matchTime from df where track = '차이나 골목길 대질주'")

# COMMAND ----------

import plotly.express as px
aPanda = a.toPandas()
fig = px.histogram(aPanda, x="p_matchTime")
fig.show()

# COMMAND ----------

b = spark.sql("select track, p_matchTime from df where track = '브로디 비밀의 연구소'")
bPanda = b.toPandas()
fig = px.histogram(bPanda, x="p_matchTime")
fig.show()

# COMMAND ----------

c = spark.sql("select track, p_matchTime from df where track = '차이나 용의 운하'")
cPanda = c.toPandas()
fig = px.histogram(cPanda, x="p_matchTime")
fig.show()

# COMMAND ----------

d = spark.sql("select track, p_matchTime from df where track = '빌리지 운명의 다리'")
dPanda = d.toPandas()
fig = px.histogram(dPanda, x="p_matchTime")
fig.show()

# COMMAND ----------

e = spark.sql("select track, p_matchTime from df where track = '대저택 은밀한 지하실'")
ePanda = e.toPandas()
fig = px.histogram(ePanda, x="p_matchTime")
fig.show()

# COMMAND ----------

f = spark.sql("select track, p_matchTime from df where track = '코리아 롯데월드 어드벤처'")
fPanda = f.toPandas()
fig = px.histogram(fPanda, x="p_matchTime", m_bin = 20)
fig['layout'].update(width=1080)
fig['layout'].update(height=780)
fig.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(track) as track_cnt from df group by track

# COMMAND ----------

f.registerTempTable("f")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchTime, count(*) from f group by p_matchTime

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 이상치 모델 시작 ㅎㅎ (6개)

# COMMAND ----------

print(df.dtypes)

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

df2.display()

# COMMAND ----------

feature = ['channelName', 'gameSpeed', 'teamPlayers', 'p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

# COMMAND ----------

df2.registerTempTable("df2")
df3 = spark.sql("select p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime from df2")
df3.display()

# COMMAND ----------

panda = df3.toPandas()

# COMMAND ----------

#del panda['rs']
#del panda['trackId']
import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

num_cols = ['playTime', 'actualdistance', 'retiredRatio' ]

# COMMAND ----------

###59초
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

import numpy as np
np.log1p(panda['p_matchTime']) 

# COMMAND ----------

panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 
panda['p_matchTime']

# COMMAND ----------

panda[num_columns]

# COMMAND ----------

import mlflow.sklearn
mlflow.sklearn.autolog()

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

panda

# COMMAND ----------

import pandas as pd
pd.concat((panda,pd.get_dummies(panda, columns=cat_columns, drop_first=True)), axis=1) #model에 drop 여부 확인

# COMMAND ----------

data = pd.get_dummies(panda, columns=cat_columns, drop_first=True)
data

# COMMAND ----------

del data['playTime']

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

form

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

### playtime포함
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

form2 = 'p_matchTime ~actualdistance+retiredRatio+playTime+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4+teamPlayers_1+teamPlayers_2+teamPlayers_3+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+difficulty_1+difficulty_2'

# COMMAND ----------

result = sm.ols(formula = form2, data = data ).fit()
print(result.summary())

# COMMAND ----------



# COMMAND ----------

sklearn.preprocessing.RobustScaler

# COMMAND ----------



# COMMAND ----------

