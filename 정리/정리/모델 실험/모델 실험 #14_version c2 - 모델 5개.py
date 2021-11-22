# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionC.csv"))

df.registerTempTable("df")

# COMMAND ----------

### validation 가능할지 확인
df = spark.sql("""select track, p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime from df where track != '코리아 롯데월드 어드벤처' """)
df.display()

# COMMAND ----------

df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) from df group by track

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
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))


print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()
import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)
num_cols = ['playTime', 'actualdistance', 'retiredRatio' ]

# COMMAND ----------

### Standard Scaler
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
#scaler.fit(numeric_data)

panda[num_cols] = scaler.fit(panda[num_cols]).transform(panda[num_cols])

# COMMAND ----------

### log transformation
import numpy as np
panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 

# COMMAND ----------

import mlflow.sklearn
mlflow.sklearn.autolog()

# COMMAND ----------

### label encoding
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

import pandas as pd
data = pd.get_dummies(panda, columns=cat_columns, drop_first=True)
data

# COMMAND ----------

import pandas as pd
data = pd.get_dummies(panda, columns=cat_columns, drop_first=False)
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

### drop first row = False
import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

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

######## drop first row = flase   & no playtime  
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

https://notebook.community/DistrictDataLabs/yellowbrick/examples/bbengfort/cooks_distance

# COMMAND ----------

# MAGIC %matplotlib notebook
# MAGIC 
# MAGIC import scipy as sp
# MAGIC import numpy as np
# MAGIC import pandas as pd 
# MAGIC import matplotlib.pyplot as plt 
# MAGIC 
# MAGIC # Note: statsmodels requires scipy 1.2
# MAGIC import statsmodels.formula.api as sm
# MAGIC 
# MAGIC from sklearn.datasets import make_regression
# MAGIC from sklearn.linear_model import LinearRegression
# MAGIC from statsmodels.stats.outliers_influence import OLSInfluence as influence
# MAGIC 
# MAGIC from yellowbrick.base import Visualizer

# COMMAND ----------

inf = influence(result)
C, P = inf.cooks_distance

# COMMAND ----------

def plot_cooks_distance(c):
    _, ax = plt.subplots(figsize=(9,6))
    ax.stem(c, markerfmt=",")
    ax.set_xlabel("instance")
    ax.set_ylabel("distance")
    ax.set_title("Cook's Distance Outlier Detection")
    return ax


plot_cooks_distance(C)

# COMMAND ----------

class CooksDistance(Visualizer):
    
    def fit(self, X, y):
        # Leverage is computed as the diagonal of the projection matrix of X 
        # TODO: whiten X before computing leverage
        self.leverage_ = (X * np.linalg.pinv(X).T).sum(1)
        
        # Compute the MSE
        rank = np.linalg.matrix_rank(X)
        df = X.shape[0] - rank
        
        resid = y - LinearRegression().fit(X, y).predict(X)
        mse = np.dot(resid, resid) / df 
        
        resid_studentized_internal = resid / np.sqrt(mse) / np.sqrt(1-self.leverage_)
        
        self.distance_ = resid_studentized_internal**2 / X.shape[1]
        self.distance_ *= self.leverage_ / (1 - self.leverage_)

        self.p_values_ = sp.stats.f.sf(self.distance_, X.shape[1], df)
        
        self.draw()
        return self
    
    
    def draw(self):
        self.ax.stem(self.distance_, markerfmt=",", label="influence")
        self.ax.axhline(4/len(self.distance_), c='r', ls='--', lw=1, label="$\frac{4}{n}$")
    
    def finalize(self):
        self.ax.legend()
        self.ax.set_xlabel("instance")
        self.ax.set_ylabel("influence")
        self.ax.set_title("Cook's Distance Outlier Detection")
    
    
viz = CooksDistance().fit(X, y)
viz.finalize()

# COMMAND ----------

from yellowbrick.regressor import CooksDistance
from yellowbrick.datasets import load_concrete

# Load the regression dataset
#X, y = load_concrete()
Y = data['p_matchTime']
X = data[:, ]

# Instantiate and fit the visualizer
visualizer = CooksDistance()
visualizer.fit(X, y)
visualizer.show()

# COMMAND ----------

data[:, 1:]

# COMMAND ----------

panda

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

# MAGIC %md 
# MAGIC playTime 제거한 최종 모델입니당 

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC playTime삭제했으나...... p-value 값이 느무느무 작아집니다ㅠㅠㅠㅠㅠ 그치만 이게 맞다 생각합니다. 

# COMMAND ----------

data2 = data
data2

# COMMAND ----------

del data['playTime']

# COMMAND ----------

feature = [x for x in data2.columns if x != 'p_matchTime']
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

# COMMAND ----------

result2 = sm.ols(formula = form2, data = data2 ).fit()
print(result2.summary())

# COMMAND ----------

form2

# COMMAND ----------

form3 = 'p_matchTime ~actualdistance+track_1+track_2+track_3+track_4+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4+teamPlayers_1+teamPlayers_2+teamPlayers_3+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+difficulty_1+retiredRatio_1+retiredRatio_2+retiredRatio_3+retiredRatio_4'

# COMMAND ----------

result2 = sm.ols(formula = form3, data = data2 ).fit()
print(result2.summary())

# COMMAND ----------

form3 = 'p_matchTime ~actualdistance+track_1+track_2+track_3+track_4+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4+teamPlayers_1+teamPlayers_2+teamPlayers_3+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+difficulty_1+retiredRatio_1+retiredRatio_2+retiredRatio_3'

# COMMAND ----------

result2 = sm.ols(formula = form3, data = data2 ).fit()
print(result2.summary())

# COMMAND ----------

form3 = 'p_matchTime ~actualdistance+track_1+track_3+track_4+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4+teamPlayers_1+teamPlayers_2+teamPlayers_3+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+difficulty_1+retiredRatio_1+retiredRatio_2'

# COMMAND ----------

result2 = sm.ols(formula = form3, data = data2 ).fit()
print(result2.summary())

# COMMAND ----------

result2 = sm.ols(formula = form3, data = data2 ).fit()
print(result2.summary())

# COMMAND ----------

