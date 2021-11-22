# Databricks notebook source
# from pyspark.sql.functions import col
# df = df.withColumn("logvalue", log10(col("prediction_column"))

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september5.csv"))

september.registerTempTable("september")

# COMMAND ----------

### sci py 의 boxcox 변환이 pyspark에서 보이지... 않아.... log1p 사용했습니다. 
data = spark.sql("select channelName, playTime, log1p(p_matchTime) as p_matchTime, trackId, teamPlayers, difficulty, actualdistance from august")
data.registerTempTable("data")

# COMMAND ----------

test = spark.sql("select channelName, playTime, log1p(p_matchTime) as p_matchTime, trackId, teamPlayers, difficulty from september")
test.registerTempTable("test")

# COMMAND ----------

data = spark.sql("select channelName, playTime, p_matchTime, trackId, teamPlayers, difficulty from august")
data.registerTempTable("data")
test = spark.sql("select channelName, playTime, p_matchTime, trackId, teamPlayers, difficulty from september")
test.registerTempTable("test")

# COMMAND ----------

data = data.filter(data.difficulty. isNotNull())
test = test.filter(test.difficulty. isNotNull())

# COMMAND ----------

data.display()

# COMMAND ----------

data.display()

# COMMAND ----------

test.display()

# COMMAND ----------

test.display()

# COMMAND ----------

categorical_cols = ['channelName', 'trackId', 'teamPlayers' , 'difficulty']
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

# Using a Spark MLLib pipeline to apply all the stages of transformation
from pyspark.ml import Pipeline
import pandas as pd
cols = data.columns
pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(data)
data = pipelineModel.transform(data)
selectedCols = ['features']+cols
data = data.select(selectedCols)
pd.DataFrame(data.take(5), columns=data.columns)

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

#Select only Features and Label from previous dataset as we need these two entities for building machine learning model
train_dataset = data.select("features","p_matchTime")
train_dataset.show()

# COMMAND ----------

#Select only Features and Label from previous dataset as we need these two entities for building machine learning model
test_dataset = test.select("features","p_matchTime")
test_dataset.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 잠시테스트 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

from pyspark.ml.functions import vector_to_array

df = finalized_data.withColumn('features', vector_to_array('features'))
df

# COMMAND ----------

df.display()

# COMMAND ----------

df.registerTempTable("df")

# COMMAND ----------

from pyspark.sql.functions import size

df = df.withColumn('features_size', size('features'))

# COMMAND ----------

df.display()

# COMMAND ----------

df["features"][0]

# COMMAND ----------

# MAGIC %md
# MAGIC 잠시테스트 완료------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

#Import Linear Regression class called LinearRegression
from pyspark.ml.regression import LinearRegression

# COMMAND ----------

#Create the Multiple Linear Regression object named MLR having feature column as features and Label column as Profit
MLR = LinearRegression(featuresCol="features", labelCol="p_matchTime")

# COMMAND ----------

### https://stackoverflow.com/questions/26892389/org-apache-spark-sparkexception-job-aborted-due-to-stage-failure-task-from-app

# COMMAND ----------

#Train the model on the training using fit() method.
model = MLR.fit(train_dataset)

# COMMAND ----------

#Predict the Profit on Test Dataset using the evulate method
pred = model.evaluate(test_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark GLM 

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
evaluation = RegressionEvaluator(labelCol="Profit", predictionCol="prediction")

# r2 - coefficient of determination
r2 = evaluation.evaluate(pred.predictions, {evaluation.metricName: "r2"})
print("r2: %.3f" %r2)

# COMMAND ----------

#Create Unlabeled dataset  to contain only feature column
unlabeled_dataset = test_dataset.select('features')

# COMMAND ----------

#Display the content of unlabeled_dataset
unlabeled_dataset.show()

# COMMAND ----------

#Predict the model output for fresh & unseen test data using transform() method
new_predictions = model.transform(unlabeled_dataset)

# COMMAND ----------

#Display the new prediction values
new_predictions.show()

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