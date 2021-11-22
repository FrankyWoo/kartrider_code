# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

trackdata = spark.sql("select track, count(distinct matchId) as cnt, collect_set(difficulty)[0] as difficulty, collect_set(lap2)[0] as lap2, collect_set(distance2)[0] as distance, collect_set(actualdistance)[0] as actualdistance, avg(p_matchTime) as matchtimeavg from august group by track")
trackdata = trackdata.filter(trackdata.track.isNotNull())
trackdata = trackdata.select("*").toPandas()
trackdata = trackdata[trackdata['actualdistance'].notna()]
#trackdata.drop(['track'], axis='columns', inplace=True)

# COMMAND ----------

import numpy as np
import pandas as pd
names = trackdata['track']
Y = trackdata['matchtimeavg']
X = trackdata.drop(['track'],axis=1)

# COMMAND ----------

### 보고서 캡처용 
fig.update_layout(width=1800, height=800)
fig.show()

# COMMAND ----------

import plotly.figure_factory as ff

import numpy as np

fig = ff.create_dendrogram(X,  labels=names.values)
fig.update_layout(width=3000, height=1000)
fig.show()

# COMMAND ----------

X

# COMMAND ----------

# MAGIC %md
# MAGIC #### k-means 
# MAGIC 댄드로그램 참고하여 clustering 개수 => 5로 지정
# MAGIC (결과 확인 후, clustering 정확도를 위한  chi-square tests는 진행하지 않기로 함.</br>
# MAGIC 너무........ 변수 많음...맵을 최종 모델에 클러스터링 할 것이 아닌 
# MAGIC 통계 검정을 위한 맵 선택이 목적이기 때문!) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 실루엣 디스턴스 => k개수 정하기. 

# COMMAND ----------

import pandas as pd # dataset load
from sklearn.cluster import KMeans # model
model = KMeans(n_clusters=5, random_state=0, algorithm='auto')
model.fit(X)

# COMMAND ----------

pred = model.predict(X)
pred

# COMMAND ----------

len(pred)

# COMMAND ----------

pred[2]

# COMMAND ----------

type(pred)

# COMMAND ----------

data = pd.DataFrame(pred, columns = ['pred'])
data

# COMMAND ----------

trackdata

# COMMAND ----------

mergedDf = trackdata.merge(data, left_index=True, right_index=True)
mergedDf

# COMMAND ----------

mergedDf[mergedDf['pred']==0]

# COMMAND ----------

mergedDf[mergedDf['pred']==1]

# COMMAND ----------

mergedDf[mergedDf['pred']==2]

# COMMAND ----------

mergedDf[mergedDf['pred']==3]

# COMMAND ----------

mergedDf[mergedDf['pred']==4]

# COMMAND ----------

mergedDf[mergedDf['track']=='빌리지 고가의 질주']

# COMMAND ----------

mergedDf[mergedDf['track']=='빌리지 손가락']

# COMMAND ----------

# MAGIC %md
# MAGIC ### 통계 검정할 최종 track 5개 or 6개 선정 
# MAGIC * 차이나 서안 병마용	
# MAGIC * 비치 해변 드라이브	
# MAGIC * 코리아 롯데월드 어드벤처
# MAGIC * 빌리지 고가의 질주 & 빌리지 손가락
# MAGIC * 네모 산타의 비밀공간