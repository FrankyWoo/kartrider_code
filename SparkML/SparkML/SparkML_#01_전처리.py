# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

### 선별 
df = spark.sql("select * from august where track in (select track from august group by track having count(distinct matchId) > 40000  )")

# COMMAND ----------

df.groupBy('track').count().show()

# COMMAND ----------

df.registerTempTable("df")
pandas = df.select("*").toPandas()

# COMMAND ----------

### one hot encoding을 위해 숫자 간추리기. 
from pyspark.ml.feature import OneHotEncoder, StringIndexer

1b1Indxr = StringIndexer().setInputCol("track").setOutputCol("trackInd")

# COMMAND ----------

#import required library
from pyspark.ml.feature import StringIndexer

track_indexer = StringIndexer(inputCol="track", outputCol="trackIndex")
#Fits a model to the input dataset with optional parameters.
df1 = track_indexer.fit(df).transform(df)

# COMMAND ----------

df1.display()

# COMMAND ----------

channel_indexer = StringIndexer(inputCol="channelName", outputCol="channelIndex")
#Fits a model to the input dataset with optional parameters.
df2 = channel_indexer.fit(df).transform(df)

# COMMAND ----------

players_indexer = StringIndexer(inputCol="teamPlayers", outputCol="playerIndex")
#Fits a model to the input dataset with optional parameters.
df3 = players_indexer.fit(df).transform(df)

# COMMAND ----------

from pyspark.ml import Pipeline

# track_indexer = StringIndexer(inputCol="qualification", outputCol="qualificationIndex")
# channel_indexer = StringIndexer(inputCol="gender", outputCol="genderIndex")
# players_indexer = StringIndexer(inputCol="gender", outputCol="genderIndex")
pipeline = Pipeline(stages=[track_indexer, channel_indexer])
model = pipeline.fit(df).transform(df)

# COMMAND ----------

model.display()

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder


track_indexer = StringIndexer(inputCol="track", outputCol="trackIndex")
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

# COMMAND ----------

df_transformed = pipeline.fit(df).transform(df)

# COMMAND ----------

df_transformed.display()

# COMMAND ----------

catCols = [x for (x, dataType) in df.dtypes if dataType == 'string']
numCols = [
  x for (x, dataType) in df.dtypes if ((dataType == "double") & (x != "isFraud"))
]

# COMMAND ----------

print(catCols)

# COMMAND ----------

print(numCols)

# COMMAND ----------

### p_matchTime과 playTime 변수 scaling 하기. 

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler

# vector_assembler = VectorAssembler(inputCols = "playTime", outputCol = 'vec_playTime')
# temp_assembler = vector_assembler.transform(df_transformed)
standard_scaler = StandardScaler(inputCol = "vec_playTime", outputCol = "scaled_playTime", withStd = True, withMean=True)
scaler_model = scaler.fit(temp_assembler)
scaled_data = scaler_model.transform(temp_assembler)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler

# vector_assembler = VectorAssembler(inputCols = "playTime", outputCol = 'vec_playTime')
# temp_assembler = vector_assembler.transform(df_transformed)
standard_scaler = StandardScaler(inputCol = "playTime", outputCol = "scaled_playTime", withStd = True, withMean=True)
scaler_model = scaler.fit(df_transformed)
scaled_data = scaler_model.transform(df_transformed)

# COMMAND ----------

scaled_data.display()

# COMMAND ----------

### playTime을 벡터로 변환 
from pyspark.ml.feature import VectorAssembler
va = VectorAssembler().setInputCols(['playTime'])
va.transform(df_transformed).display()

# COMMAND ----------

vec = va.transform(df_transformed)

# COMMAND ----------

standard_scaler = StandardScaler(inputCol ="VectorAssembler_2c15438ee406__output", outputCol = "scaled_playTime", withStd = True, withMean=True)
scaler_model = scaler.fit(vec)
scaled_data = scaler_model.transform(vec)

# COMMAND ----------

scaled_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 아래는 scaling을 범주형에도 코드 돌려버림...

# COMMAND ----------

##df_transformed.toPandas()

# COMMAND ----------

df_transformed.collect()[0]

# COMMAND ----------

# importing some libraries

import numpy as np
from pyspark.sql import functions as F
# from pyspark.sql import SQLContext
# sqlContext = SQLContext(sc)

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
# checking if spark context is already created
#print(sc.version)

# COMMAND ----------

# empty dictionary d
d = {}
# Fill in the entries one by one
for col in df.columns[1:-3]:
      d[col] = df.approxQuantile(col,[0.01,0.99],0.25)
      print(col+" done")

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(inputCol="track_vec", 
                        outputCol="scaled_track",
                        withStd=True,withMean=True)
scaler_model = scaler.fit(df_transformed)
scaled_data = scaler_model.transform(df_transformed)

# COMMAND ----------

scaled_data.display()

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(inputCol="channel_vec", 
                        outputCol="scaled_channel",
                        withStd=True,withMean=True)
scaler_model = scaler.fit(scaled_data)
scaled_data = scaler_model.transform(scaled_data)

# COMMAND ----------

scaler = StandardScaler(inputCol="players_vec", 
                        outputCol="scaled_players",
                        withStd=True,withMean=True)
scaler_model = scaler.fit(scaled_data)
scaled_data = scaler_model.transform(scaled_data)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler
columns_to_scale = ["track_vec", "channel_vec", "players_vec"]
#assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
scalers = [MinMaxScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
pipeline = Pipeline(stages=assemblers + scalers)

scalerModel = pipeline.fit(df_transformed)
scaledData = scalerModel.transform(df_transformed)

# COMMAND ----------

from pyspark.ml import Pipeline
#from pyspark.ml.feature import MinMaxScaler
columns_to_scale = ["track_vec", "channel_vec", "players_vec"]
#assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
scalers = StandardScaler((inputCol= col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale,  withStd=True,withMean=True)
#pipeline = Pipeline(stages=assemblers + scalers)

scalerModel = scalers.fit(df_transformed)
scaledData = scalerModel.transform(df_transformed)




# COMMAND ----------

catCols = [x for (x, dataType) in df.dtypes if dataType == 'string']
numCols = [
  x for (x, dataType) in df.dtypes if ((dataType == "double") & (x != "isFraud"))
]

# COMMAND ----------

print(numCols)

# COMMAND ----------

print(catCols)

# COMMAND ----------

from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(inputCol="track_vec", 
                        outputCol="scaled_track",
                        withStd=True,withMean=True)
scaler_model = scaler.fit(df_transformed)
scaled_data = scaler_model.transform(df_transformed)