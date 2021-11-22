# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/upperlower.csv"))

df.registerTempTable("df")

# COMMAND ----------

df = spark.sql("select * from df where channelName != 'grandprix_speedTeamInfinit'")
df.registerTempTable("df")

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

categoricals = [var for var in df2.columns if var.endswith("_one_hot")]
num = ["numericals"]
vector_assembler = VectorAssembler(inputCols= categoricals + num, outputCol= "features")
df2 = vector_assembler.transform(df2)

# COMMAND ----------

dataset2 = df2.withColumnRenamed('p_matchTime', 'label')
dataset2.display()

# COMMAND ----------

dataset2 = dataset2.select("features", "label")
dataset2.display()

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
print("r2: %f" % trainingSummary.r2)