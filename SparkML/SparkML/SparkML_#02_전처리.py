# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

df = spark.sql("select channelName, playTime, p_matchTime, trackId, teamPlayers from august")
df.registerTempTable("df")

# COMMAND ----------

# MAGIC %md
# MAGIC 원핫인코딩 : track, channelName, teamPlayers

# COMMAND ----------

### track, channelName, teamPlayers에 대한 원핫인코딩
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline


track_indexer = StringIndexer(inputCol="trackId", outputCol="trackIndex")
channel_indexer = StringIndexer(inputCol="channelName", outputCol="channelIndex")
players_indexer = StringIndexer(inputCol="teamPlayers", outputCol="playerIndex")


onehotencoder_track_vector = OneHotEncoder(inputCol="trackIndex", outputCol="track_vec")
onehotencoder_channel_vector = OneHotEncoder(inputCol="channelIndex", outputCol="channel_vec")
onehotencoder_players_vector = OneHotEncoder(inputCol="playerIndex", outputCol="players_vec")
#Create pipeline and pass all stages
pipeline = Pipeline(stages=[track_indexer,
                            channel_indexer,
                            players_indexer,
                            onehotencoder_track_vector,
                            onehotencoder_channel_vector,
                            onehotencoder_players_vector
                    ])

df_transformed = pipeline.fit(august).transform(august)

# COMMAND ----------

df_transformed.display()

# COMMAND ----------

import pyspark.sql.functions as F

print(august.where(F.col('channelName').isNull()).count())
print(august.where(F.col('track').isNull()).count())
print(august.where(F.col('teamPlayers').isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC standard scaling

# COMMAND ----------

### playTime을 벡터로 변환 
from pyspark.ml.feature import VectorAssembler
va = VectorAssembler().setInputCols(['playTime'])
va.transform(df_transformed).display()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler
vec = va.transform(df_transformed)
standard_scaler = StandardScaler(inputCol ="VectorAssembler_356a97da5a38__output", outputCol = "scaled_playTime", withStd = True, withMean=True)
scaler_model = standard_scaler.fit(vec)
scaled_data = scaler_model.transform(vec)

# COMMAND ----------

scaled_data.display()

# COMMAND ----------

#데이터프레임 csv로도 저장
import pyspark
from pyspark.sql.functions import *

#scaled_data.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/onehotandscaling.csv")