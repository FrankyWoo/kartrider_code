# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionC.csv"))

df.registerTempTable("df")

# COMMAND ----------

df = spark.sql("select p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime from df")
df.display()

# COMMAND ----------



# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("retiredRatio",col("retiredRatio").cast(StringType()))\
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))


print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()

# COMMAND ----------

import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

numeric_data = panda[num_columns].values
numeric_data

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaler.fit(numeric_data)

# COMMAND ----------

from sklearn.preprocessing import RobustScaler
transformer = RobustScaler().fit(numeric_data)
print(transformer)
panda[num_columns] = transformer.transform(panda[num_columns])

# COMMAND ----------

panda

# COMMAND ----------

import mlflow.sklearn
mlflow.sklearn.autolog()

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

import pandas as pd
pd.concat((panda,pd.get_dummies(panda, columns=cat_columns, drop_first=True)), axis=1) #model에 drop 여부 확인

# COMMAND ----------

data = pd.get_dummies(panda, columns=cat_columns, drop_first=True)
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

form

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

### 스케일링 안 함 
import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------



# COMMAND ----------

panda.corr()

# COMMAND ----------

import statsmodels.api as sm

model = sm.formula.glm(form,
                       family=sm.families.Binomial(), data=data).fit()
print(model.summary())



# glm_result = sm.GLM(formula = form, data = data,  family=sm.families.Gamma()).fit()
# print(glm_result.summary())