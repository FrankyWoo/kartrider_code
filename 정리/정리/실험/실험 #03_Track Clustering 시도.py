# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

print(august.count())

# COMMAND ----------

august2 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august5.csv"))

august2.registerTempTable("august2")

# COMMAND ----------

print(august2.count())

# COMMAND ----------

august.display()

# COMMAND ----------

 august.describe('p_matchTime').show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### track 군집 분석 </br>
# MAGIC 통계 분석에 사용될 맵 선정을 위해 실행 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(distinct matchId) as cnt, collect_set(difficulty)[0] as difficulty, collect_set(lap2)[0] as lap2, collect_set(distance2)[0] as distance, 
# MAGIC collect_set(actualdistance)[0] as actualdistance, avg(p_matchTime) as matchtimeavg from august group by track

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(distinct matchId) as cnt, collect_set(difficulty)[0] as difficulty, collect_set(lap2)[0] as lap2, collect_set(distance2)[0] as distance, 
# MAGIC round(collect_set(actualdistance)[0], 5) as actualdistance, round(avg(p_matchTime),5) as matchtimeavg from august group by track

# COMMAND ----------

trackdata = spark.sql("select track, count(distinct matchId) as cnt, collect_set(difficulty)[0] as difficulty, round(collect_set(lap2)[0]) as lap2, collect_set(distance2)[0] as distance,round(collect_set(actualdistance)[0]) as actualdistance, round(avg(p_matchTime) as matchtimeavg from august group by track")

# COMMAND ----------

trackdata.display()

# COMMAND ----------

trackdata = spark.sql("select track, count(distinct matchId) as cnt, collect_set(difficulty)[0] as difficulty, collect_set(lap2)[0] as lap2, collect_set(distance2)[0] as distance, round(collect_set(actualdistance)[0]) as actualdistance, round(avg(p_matchTime),5) as matchtimeavg from august group by track")
trackdata = trackdata.filter(trackdata.track.isNotNull())
trackdata = trackdata.select("*").toPandas()
#trackdata.drop(['track'], axis='columns', inplace=True)

# COMMAND ----------

print(type(trackdata['actualdistance'][0]))
print(type(trackdata['lap2'][0]))

# COMMAND ----------

trackdata = trackdata[trackdata['actualdistance'].notna()]

# COMMAND ----------

import numpy as np
trackdata['actualdistance'].apply(np.int64)

# COMMAND ----------

X

# COMMAND ----------

names = trackdata['track']
Y = trackdata['matchtimeavg']
X = trackdata.drop(['track','distance'],axis=1)

# COMMAND ----------

names.values

# COMMAND ----------

X

# COMMAND ----------

import plotly.figure_factory as ff

import numpy as np

#X = np.random.rand(10, 12)
#names = ['Jack', 'Oxana', 'John', 'Chelsea', 'Mark', 'Alice', 'Charlie', 'Rob', 'Lisa', 'Lily']
fig = ff.create_dendrogram(X,  labels=names.values)
fig.update_layout(width=3000, height=800)
fig.show()

# COMMAND ----------



# COMMAND ----------

### 보고서 캡처용 
fig.update_layout(width=1800, height=800)
fig.show()

# COMMAND ----------

matchtimeavg제거 

# COMMAND ----------

X

# COMMAND ----------

X = trackdata.drop(['track','distance', 'matchtimeavg'],axis=1)
fig = ff.create_dendrogram(X,  labels=names.values)
fig.update_layout(width=3000, height=800)
fig.show()

# COMMAND ----------



# COMMAND ----------

fig.update_layout(width=1800, height=800)
fig.show()

# COMMAND ----------

import pandas as pd
###p_matchTime에 대한 lap2, actualdistance, distinct matchId, difficulty!
### Anova test를 위한 map clustering. 

# COMMAND ----------

trackinfo = spark.sql("select track, count(distinct matchId) as cnt, collect_set(difficulty)[0] as difficulty, collect_set(lap2)[0] as lap2, collect_set(distance2)[0] as distance,collect_set(actualdistance)[0] as actualdistance, avg(p_matchTime) as matchtimeavg from august group by track")
trackinfo = trackinfo.filter(trackinfo.track. isNotNull())
trackinfo.display()

# COMMAND ----------

trackinfodf = trackinfo.select("*").toPandas()

# COMMAND ----------

trackinfodf.corr()

# COMMAND ----------

trackinfodf.drop(['matchtimeavg'], axis='columns', inplace=True)

# COMMAND ----------

trackinfodf.drop(['track'], axis='columns', inplace=True)

# COMMAND ----------

trackinfodf

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df = spark.createDataFrame(data, ["features"])

# r1 = Correlation.corr(df, "features").head()
# print("Pearson correlation matrix:\n" + str(r1[0]))

r2 = Correlation.corr(df, "features", "spearman").head()
print("Spearman correlation matrix:\n" + str(r2[0]))

# COMMAND ----------

df.show()

# COMMAND ----------

#### 군집 대상 중복 X, 자료의 크기 작음 & 몇 개로 나눌지 안 정함 => 계층적 군집 분석 
import seaborn as sns 
import scipy 
import matplotlib.pyplot as plt 
import scipy.cluster.hierarchy as sch
from scipy.cluster.hierarchy import linkage, dendrogram
from scipy.cluster.hierarchy import fcluster

# Calculate the linkage: mergings
#mergings = linkage(trackinfodf, method='complete
#mergings = linkage(trackinfodf,method='complete')


# COMMAND ----------

trackinfo = trackinfo.filter(trackinfo
                             .track. isNotNull())

# COMMAND ----------

# MAGIC %md 
# MAGIC -----------------------------------------------기타 --------- 파일 저장 관련!---------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

import pyspark.sql.functions as F
print(august.where(F.col('track').isNull()).count())

# COMMAND ----------

print(august.where(F.col('p_matchTime').isNull()).count())

# COMMAND ----------

august2 = august.filter(august.track. isNotNull())

# COMMAND ----------

print(august2.where(F.col('track').isNull()).count())

# COMMAND ----------

print(august2.where(F.col('lap2').isNull()).count())

# COMMAND ----------

#august.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/august6.csv")

# COMMAND ----------

#august2.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/august7.csv")

# COMMAND ----------

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
