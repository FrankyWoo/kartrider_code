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

import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

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

# MAGIC %md
# MAGIC Scaling : numeric features

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
#scaler.fit(numeric_data)
numeric_data = ['playTime', 'actualdistance', 'retiredRatio']
data[numeric_data] = scaler.fit(data[numeric_data]).transform(data[numeric_data])

# COMMAND ----------

numeric_data

# COMMAND ----------

# MAGIC %md 
# MAGIC Log transformation : target value 

# COMMAND ----------

### log transformation
import numpy as np
data['p_matchTime'] = np.log1p(data['p_matchTime']) 

# COMMAND ----------

del data['playTime']

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

form

# COMMAND ----------

#playtime 포함 안 함 
import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()
print(result.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC Backward elimination

# COMMAND ----------

form2 =  'p_matchTime ~actualdistance+retiredRatio+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+gameSpeed_1+gameSpeed_2+gameSpeed_4+teamPlayers_1+teamPlayers_2+teamPlayers_3+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+difficulty_1+difficulty_2'

# COMMAND ----------

result = sm.ols(formula = form2, data = data ).fit()
print(result.summary())

# COMMAND ----------

form3 =  'p_matchTime ~actualdistance+retiredRatio+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_6+channelName_7+gameSpeed_1+gameSpeed_2+gameSpeed_4+teamPlayers_1+teamPlayers_2+teamPlayers_3+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+difficulty_1+difficulty_2'

# COMMAND ----------

result = sm.ols(formula = form3, data = data ).fit()
print(result.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC Cook's distance

# COMMAND ----------

data

# COMMAND ----------

panda.columns

# COMMAND ----------

data.iloc[:, 1:]

# COMMAND ----------

from yellowbrick.regressor import CooksDistance
#from yellowbrick.datasets import load_concrete

# Load the regression dataset
#X, y = load_concrete()
y = data['p_matchTime']
X = data.iloc[:, 1:]

# Instantiate and fit the visualizer
visualizer = CooksDistance()
visualizer.fit(X, y)
visualizer.show()

# COMMAND ----------

from yellowbrick.base import Visualizer
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

from sklearn.linear_model import LinearRegression
from yellowbrick.regressor import ResidualsPlot

# Instantiate and fit the visualizer
model = LinearRegression()
visualizer_residuals = ResidualsPlot(model)
visualizer_residuals.fit(X, y)
visualizer_residuals.show()