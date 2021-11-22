# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/upperlower.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.count()

# COMMAND ----------

aa.count()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## TYPE 변환
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast('integer')) \
    .withColumn("p_matchRetired",col("p_matchRetired").cast(StringType()))

# COMMAND ----------

from collections import defaultdict

data_types = defaultdict(list)
for entry in df2.schema.fields:
    data_types[str(entry.dataType)].append(entry.name)

strings = data_types["StringType"]

# missing_data_fill = {}
# for var in strings:
#   missing_data_fill[var] = "missing"
# df2 = df2.fillna(missing_data_fill)

numericals = data_types["DoubleType"] + data_types["IntegerType"] \
                                      + data_types["LongType"]

# COMMAND ----------

response_vars = ["p_matchTime"]
variable_list_emblem = ["channelName", "p_rankinggrade2", "teamPlayers", "actualdistance","difficulty","track","gameSpeed","retired_ratio"]
df2 = df2.select(response_vars + variable_list_emblem)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

strings = [var for var in variable_list_emblem if var in data_types["StringType"]]
stage_string = [StringIndexer(inputCol= c, outputCol= c+"_string_encoded") for c in strings]
stage_one_hot = [OneHotEncoder(inputCol= c+"_string_encoded", outputCol= c+ "_one_hot") for c in strings]

ppl = Pipeline(stages= stage_string + stage_one_hot)
df2 = ppl.fit(df2).transform(df2)

from pyspark.ml.feature import Normalizer, VectorAssembler, StandardScaler

numericals = [var for var in variable_list_emblem if var not in data_types["StringType"]]
numericals_out = [var+ "_normalized" for var in numericals]

vs = VectorAssembler(inputCols= numericals, outputCol= "numericals")
df2 = vs.transform(df2)

scaler = StandardScaler(inputCol = "numericals", outputCol = "numericals_after_scale")
normalizer = Normalizer(inputCol = "numericals_after_scale", outputCol= "normalized_numericals", p=1.0)

ppl2 = Pipeline(stages= [scaler, normalizer])
df2 = ppl2.fit(df2).transform(df2)

# COMMAND ----------

dataset2.display()

# COMMAND ----------

categoricals = [var for var in df2.columns if var.endswith("_one_hot")]
num = ["numericals"]
vector_assembler = VectorAssembler(inputCols= categoricals + num, outputCol= "features")
df2 = vector_assembler.transform(df2)

# COMMAND ----------

dataset2 = df2.withColumnRenamed('p_matchTime', 'label')
dataset2.display()

# COMMAND ----------

###drop 
cols = ("channelName","track","p_rankinggrade2_string_encoded", "teamPlayers_string_encoded", "gameSpeed_string_encoded", "channelName_one_hot","p_rankinggrade2_one_hot", "teamPlayers_one_hot")

dataset3 = dataset2.drop(*cols) 

# COMMAND ----------

cols = ("track_one_hot","gameSpeed_one_hot","numericals", "numericals_after_scale", "normalized_numericals", "features","p_rankinggrade2_one_hot", "teamPlayers_one_hot")

dataset3 = dataset3.drop(*cols) 

# COMMAND ----------

dataset3.display()

# COMMAND ----------

#dataset3.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/dataset3.csv")

# COMMAND ----------

###정규화

# COMMAND ----------

dataset2 = dataset2.select("features", "label")
dataset2.display()

# COMMAND ----------

from pyspark.ml.functions import vector_to_array

final = dataset2.withColumn('features', vector_to_array('features'))
final

# COMMAND ----------

final.display()

# COMMAND ----------

from pyspark.ml.regression import LinearRegression


lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(dataset2)

# Print the coefficients and intercept for linear regression
# print("Coefficients: %s" % str(lrModel.coefficients))
# print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2) ### adjusted R^2 
print("adusted r2: %f" % trainingSummary.r2adj)
print("adusted r2: %f" % trainingSummary.pValues)

# COMMAND ----------

import sys

from abc import ABCMeta

from pyspark import keyword_only, since
from pyspark.ml import Predictor, PredictionModel
from pyspark.ml.base import _PredictorParams
from pyspark.ml.param.shared import HasFeaturesCol, HasLabelCol, HasPredictionCol, HasWeightCol, \
    Param, Params, TypeConverters, HasMaxIter, HasTol, HasFitIntercept, HasAggregationDepth, \
    HasMaxBlockSizeInMB, HasRegParam, HasSolver, HasStepSize, HasSeed, HasElasticNetParam, \
    HasStandardization, HasLoss, HasVarianceCol
from pyspark.ml.tree import _DecisionTreeModel, _DecisionTreeParams, \
    _TreeEnsembleModel, _RandomForestParams, _GBTParams, _TreeRegressorParams
from pyspark.ml.util import JavaMLWritable, JavaMLReadable, HasTrainingSummary, \
    GeneralJavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, \
    JavaPredictor, JavaPredictionModel, JavaWrapper
from pyspark.ml.common import inherit_doc
from pyspark.sql import DataFrame

# COMMAND ----------

__all__ = ['AFTSurvivalRegression', 'AFTSurvivalRegressionModel',
           'DecisionTreeRegressor', 'DecisionTreeRegressionModel',
           'GBTRegressor', 'GBTRegressionModel',
           'GeneralizedLinearRegression', 'GeneralizedLinearRegressionModel',
           'GeneralizedLinearRegressionSummary', 'GeneralizedLinearRegressionTrainingSummary',
           'IsotonicRegression', 'IsotonicRegressionModel',
           'LinearRegression', 'LinearRegressionModel',
           'LinearRegressionSummary', 'LinearRegressionTrainingSummary',
           'RandomForestRegressor', 'RandomForestRegressionModel',
           'FMRegressor', 'FMRegressionModel']


# COMMAND ----------

@property
def pValues(self):
  return self._call_java("pValues")


# COMMAND ----------

fit = sm.OLS("label", "features").fit()
for attributeIndex in range (0, numberOfAttributes):
    print(fit.pvalues[attributeIndex])

# COMMAND ----------

# MAGIC %r
# MAGIC library("sparklyr")

# COMMAND ----------

# MAGIC %r
# MAGIC sc <- spark_connect(method = "databricks", spark_home = "<spark-home-path>")

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC R_df <- read.df("/FileStore/KartRider/upperlower.csv", source = "csv", header="true", inferSchema = "true")
# MAGIC head(R_df)

# COMMAND ----------

# MAGIC %r
# MAGIC model <- spark.lm(R_df, p_matchTime ~ actualdistance + retired_ratio , regParam = 0.01, maxIter = 1)

# COMMAND ----------

# MAGIC %r
# MAGIC summary(model)

# COMMAND ----------

# Fit a linear regression model
model <- spark.lm(training, label ~ features, regParam = 0.3, elasticNetParam = 0.8)

# Prediction
predictions <- predict(model, test)
head(predictions)

# Summarize
summary(model)

# COMMAND ----------

dataset3.createOrReplaceTempView("dataset3")

# COMMAND ----------

# MAGIC %r
# MAGIC createOrReplaceTempView(dataset3, "dataset3")

# COMMAND ----------

# MAGIC %r
# MAGIC dataset3 <- sql("SELECT * FROM dataset3")

# COMMAND ----------

# MAGIC %r
# MAGIC head(dataset3)

# COMMAND ----------

dataset3.columns

# COMMAND ----------

# MAGIC %r
# MAGIC #### category 변수 one-hot encoding 이전
# MAGIC 
# MAGIC # Fit a linear regression model
# MAGIC model <- spark.lm(dataset3, label ~ p_rankinggrade2+teamPlayers + actualdistance +difficulty + gameSpeed + retired_ratio + channelName_string_encoded +  track_string_encoded, regParam = 0.3, elasticNetParam = 0.8)
# MAGIC #model <- spark.lm(dataset3, label ~ ., regParam = 0.3, elasticNetParam = 0.8)
# MAGIC # Prediction
# MAGIC # predictions <- predict(model, test)
# MAGIC # head(predictions)
# MAGIC 
# MAGIC # Summarize
# MAGIC summary(model)

# COMMAND ----------

dataset2.createOrReplaceTempView("dataset2")

# COMMAND ----------

# MAGIC %r
# MAGIC dataset2 <- sql("SELECT * FROM dataset2")

# COMMAND ----------

# MAGIC %r
# MAGIC #### category 변수 vector 변환한 것
# MAGIC 
# MAGIC # Fit a linear regression model
# MAGIC model <- spark.lm(dataset2, label ~ features, regParam = 0.3, elasticNetParam = 0.8)
# MAGIC #model <- spark.lm(dataset3, label ~ ., regParam = 0.3, elasticNetParam = 0.8)
# MAGIC # Prediction
# MAGIC # predictions <- predict(model, test)
# MAGIC # head(predictions)
# MAGIC 
# MAGIC # Summarize
# MAGIC summary(model)