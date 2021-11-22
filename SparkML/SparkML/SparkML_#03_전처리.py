# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

data = spark.sql("select channelName, playTime, p_matchTime, trackId, teamPlayers from august")
data.registerTempTable("data")

# COMMAND ----------

#Create features storing categorical & numerical variables, omitting the last column
categorical_cols = [item[0] for item in data.dtypes if item[1].startswith('string')]
print(categorical_cols)

numerical_cols = [item[0] for item in data.dtypes if item[1].startswith('int') | item[1].startswith('double')][:-1]
print(numerical_cols)

# COMMAND ----------

categorical_cols = ['channelName', 'trackId', 'teamPlayers']
numerical_cols = ['playTime']
print(categorical_cols)
print(numerical_cols)

# COMMAND ----------

# First using StringIndexer to convert string/text values into numerical values followed by OneHotEncoderEstimator 
# Spark MLLibto convert each Stringindexed or transformed values into One Hot Encoded values.
# VectorAssembler is being used to assemble all the features into one vector from multiple columns that contain type double 
# Also appending every step of the process in a stages array
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
stages = []
for categoricalCol in categorical_cols:
    print(categoricalCol)
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    OHencoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "_catVec"])
    stages += [stringIndexer, OHencoder]
assemblerInputs = [c + "_catVec" for c in categorical_cols] + numerical_cols
Vectassembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [Vectassembler]

# COMMAND ----------

stages

# COMMAND ----------

stages

# COMMAND ----------

df.display()

# COMMAND ----------

# Using a Spark MLLib pipeline to apply all the stages of transformation
from pyspark.ml import Pipeline
import pandas as pd
cols = data.columns
pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(data)
data = pipelineModel.transform(data)
selectedCols = ['features']+cols
data = data.select(selectedCols)
# pd.DataFrame(list(data), columns=data.columns)

# COMMAND ----------

data.show()

# COMMAND ----------

#Select only Features and Label from previous dataset as we need these two entities for building machine learning model
finalized_data = data.select("features","p_matchTime")

finalized_data.show()

# COMMAND ----------

#Import Linear Regression class called LinearRegression
from pyspark.ml.regression import LinearRegression

#Create the Multiple Linear Regression object named MLR having feature column as features and Label column as Profit
MLR = LinearRegression(featuresCol="features", labelCol="p_matchTime")

# COMMAND ----------

#Train the model on the training using fit() method.
model = MLR.fit(finalized_data)

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september4.csv"))

september.registerTempTable("september")

# COMMAND ----------

september2 = september.filter(september.p_matchTime. isNotNull())

# COMMAND ----------

september2.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/september5.csv")

# COMMAND ----------

september2.registerTempTable("september2")

# COMMAND ----------

test = spark.sql("select channelName, playTime, p_matchTime, trackId, teamPlayers from august")
test.registerTempTable("test")

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
stages = []
for categoricalCol in categorical_cols:
    print(categoricalCol)
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    OHencoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "_catVec"])
    stages += [stringIndexer, OHencoder]
assemblerInputs = [c + "_catVec" for c in categorical_cols] + numerical_cols
Vectassembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [Vectassembler]

# COMMAND ----------

from pyspark.ml import Pipeline
import pandas as pd
cols = test.columns
pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(test)
test = pipelineModel.transform(test)
selectedCols = ['features']+cols
test = test.select(selectedCols)
pd.DataFrame(test.take(5), columns=test.columns)

# COMMAND ----------

#Predict the Profit on Test Dataset using the evulate method
pred = model.evaluate(test)

# COMMAND ----------

#Show the predicted Grade values along side actual Grade values
pred.predictions.show()

# COMMAND ----------

#Find out coefficient value
coefficient = model.coefficients
print ("The coefficients of the model are : %a" %coefficient)

# COMMAND ----------

#Find out intercept Value
intercept = model.intercept
print ("The Intercept of the model is : %f" %intercept)

# COMMAND ----------

#Evaluate the model using metric like Mean Absolute Error(MAE), Root Mean Square Error(RMSE) and R-Square
from pyspark.ml.evaluation import RegressionEvaluator
evaluation = RegressionEvaluator(labelCol="p_matchTime", predictionCol="prediction")

# r2 - coefficient of determination
r2 = evaluation.evaluate(pred.predictions, {evaluation.metricName: "r2"})
print("r2: %.3f" %r2)

# COMMAND ----------

#Create Unlabeled dataset  to contain only feature column
unlabeled_dataset = test.select('features')

# COMMAND ----------

#Display the content of unlabeled_dataset
unlabeled_dataset.show()

# COMMAND ----------

df.columns

# COMMAND ----------

import numpy as np
import pandas as pd
import os
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import lit, col
from pyspark.sql import functions as F