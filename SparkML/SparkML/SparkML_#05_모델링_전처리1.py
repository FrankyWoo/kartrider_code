# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august7.csv"))

august.registerTempTable("august")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from august

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from august where track = '빌리지 고가의 질주'

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september6.csv"))

september.registerTempTable("september")

# COMMAND ----------

september.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from september where track = '빌리지 고가의 질주'

# COMMAND ----------

df = august.union(september)
df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- grandprix_speedTeamInfinit 채널 수 확인 
# MAGIC select channelName, count(channelName) as cnt from df group by channelName

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- grandprix_speedTeamInfinit 채널 수 확인 
# MAGIC select  track, count(track) as cnt from df where channelName = 'grandprix_speedTeamInfinit' group by track
# MAGIC --- 그랑프리를 빼지않도록 하겠습니다!! 이후, 정확도를 보고 빼도록 할게요. 그랑프리전에 한해서 수가 많기 때문에.... 원핫인코딩에 그랑프리를 포함시켜볼 필요가 있다고 판단됩니다. 

# COMMAND ----------

def lgcate_trainStadScaler(nweek_dt, nweek_all, label_dt, feature_nweek, feature_category, reco_k, weight, modelname, relevancy_method, threshold, inter_yn=False):
    start = dt.datetime.now()

    ### Model Data
    nweek_dt = nweek_dt.na.fill(0)
    
    indexers = [
        StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)).setHandleInvalid("keep")
        for c in feature_category
    ]

    # The encode of indexed vlaues multiple columns
    encoders = [OneHotEncoder(dropLast=False,inputCol=indexer.getOutputCol(),
                outputCol="{0}_encoded".format(indexer.getOutputCol())) 
        for indexer in indexers
    ]
    
    vectorAssembler = VectorAssembler(inputCols=feature_nweek + [encoder.getOutputCol() for encoder in encoders], outputCol='features')
    
    pipeline  = Pipeline(stages=indexers + encoders+[vectorAssembler])
    nweek_pipeline_m = pipeline.fit(nweek_dt)
    nweek_traindata = nweek_pipeline_m.transform(nweek_dt).select(['prd_no1', 'prd_no2', 'features', 'label'])

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
    nweek_scalerModel = scaler.fit(nweek_traindata.select("features"))
    nweek_traindata = nweek_scalerModel.transform(nweek_traindata)
    
    (nweek_traindf, nweek_testdf) = nweek_traindata.randomSplit([0.8, 0.2], seed=2310)

    ### Model Traininig
    lr = GeneralizedLinearRegression(featuresCol = 'scaledFeatures', labelCol = 'label', family="binomial", link="logit", maxIter=30)
    
    nweek_lrModel = lr.fit(nweek_traindf)
    
    nweek_trainingSummary = nweek_lrModel.summary
    
    nweek_train_predictions = nweek_lrModel.transform(nweek_traindf)
    nweek_test_predictions = nweek_lrModel.transform(nweek_testdf)    

    ### Classification Model Performance    
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction")
    
    print ("The area under ROC for train set (nweek) is {}".format(evaluator.evaluate(nweek_train_predictions)))
    print ("The area under ROC for test set (nweek) is {}".format(evaluator.evaluate(nweek_test_predictions)))

    print ("Nweek Model : Coefficients: " + str(nweek_lrModel.coefficients) + "Intercept: " + str(nweek_lrModel.intercept))

#     print("Recently Model : P Values " + str(recently_trainingSummary.pValues))
#     print("Nweek Model : P Values " + str(nweek_trainingSummary.pValues))

    ### Recommend Model Performance    
    nweek_all = nweek_all.na.fill(0)
    nweek_recodata = nweek_pipeline_m.transform(nweek_all).select(['prd_no1', 'prd_no2', 'features', 'label'])
    nweek_recodata = nweek_scalerModel.transform(nweek_recodata)
    nweek_recodata = nweek_lrModel.transform(nweek_recodata)
    nweek_predictions = nweek_recodata.select('prd_no1', 'prd_no2', col('prediction').alias('prob'), element_select(col("scaledFeatures")).alias('cosco'))    
    
    nweek_predictions.createOrReplaceTempView("nweek_predictions")  
    
    prediction = spark.sql( """
        select prd_no1, prd_no2, ROW_NUMBER() OVER (PARTITION BY prd_no1 ORDER BY prob asc, cosco asc) AS score
        from nweek_predictions
    """ )
    
    rank_eval = SparkRankingEvaluation(label_dt, prediction, k = reco_k, col_user="prd_no1", col_item="prd_no2", 
                                    col_rating="truth", col_prediction="score", 
                                    relevancy_method=relevancy_method, threshold = threshold)
                                    
    print("Model: {}".format(modelname), "Top K:%d" % rank_eval.k, "MAP:%f" % rank_eval.map_at_k(), "NDCG:%f" % rank_eval.ndcg_at_k(), "Precision@K:%f" % rank_eval.precision_at_k(), "Recall@K:%f" % rank_eval.recall_at_k())
              
    end = dt.datetime.now() - start
    print (end)  
    return(nweek_lrModel, nweek_scalerModel, rank_eval, prediction, nweek_predictions, nweek_recodata)

# COMMAND ----------

#https://people.stat.sc.edu/haigang/GLM_in_spark.html

# COMMAND ----------

df.columns

# COMMAND ----------

df.select("*").show()

# COMMAND ----------

df.schema['channelName'].dataType

# COMMAND ----------

from collections import defaultdict

data_types = defaultdict(list)
for entry in df.schema.fields:
    data_types[str(entry.dataType)].append(entry.name)

strings = data_types["StringType"]

missing_data_fill = {}

# COMMAND ----------

data_types

# COMMAND ----------

### type에 따라 딕셔너리 저장
data_types = defaultdict(list)
data_types['StringType'].append('channelName')
data_types['StringType'].append('trackId')
data_types['StringType'].append('track')
data_types['StringType'].append('p_rankinggrade2')
data_types['StringType'].append('teamPlayers')
data_types['StringType'].append('p_matchRetired')
data_types['StringType'].append('gameSpeed')
data_types['StringType'].append('difficulty')

data_types['IntegerType'].append('playTime')

data_types['DoubleType'].append('p_matchTime')
data_types['DoubleType'].append('actualdistance')
data_types['DoubleType'].append('retired_ratio')
data_types

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
df.dtypes

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
# // Convert Type
df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType()))
# df.withColumn("salary",col("salary").cast("int"))
# df.withColumn("salary",col("salary").cast("integer"))

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerTyp
df2 = df.withColumn("age",col("age").cast(StringType())) \
    .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
df2.printSchema()
df2 = df.withColumn("col4", func.round(df["col3"]).cast('integer'))

# COMMAND ----------

## type converter
type(str('p_rankinggrade2'))

# COMMAND ----------

p_rankinggrade2 = str('p_rankinggrade2')
type(p_rankinggrade2)

# COMMAND ----------

stage_string = [StringIndexer(inputCol= c, outputCol= c+"_string_encoded") for c in strings]
stage_one_hot = [OneHotEncoder(inputCol= c+"_string_encoded", outputCol= c+ "_one_hot") for c in strings]

ppl = Pipeline(stages= stage_string + stage_one_hot)
df = ppl.fit(df).transform(df)

# COMMAND ----------

from pyspark.ml.feature import Normalizer, VectorAssembler, StandardScaler

numericals = [var for var in variable_list_emblem if var not in data_types["StringType"]]
numericals_out = [var+ "_normalized" for var in numericals]

vs = VectorAssembler(inputCols= numericals, outputCol= "numericals")
df = vs.transform(df)

scaler = StandardScaler(inputCol = "numericals", outputCol = "numericals_after_scale")
normalizer = Normalizer(inputCol = "numericals_after_scale", outputCol= "normalized_numericals", p=1.0)

ppl2 = Pipeline(stages= [scaler, normalizer])
df = ppl2.fit(df).transform(df)

# COMMAND ----------

# MAGIC %md
# MAGIC step01 : convert data type 

# COMMAND ----------

### types
df.dtypes

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
## difficulty
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("p_matchRetired",col("p_matchRetired").cast(StringType()))

# COMMAND ----------

df2.dtypes

# COMMAND ----------

import pyspark.sql.functions as func
df2 = df2.withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast('integer'))

# COMMAND ----------

df.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC step02 
# MAGIC 코드 적용 

# COMMAND ----------

from collections import defaultdict

data_types = defaultdict(list)
for entry in df2.schema.fields:
    data_types[str(entry.dataType)].append(entry.name)

strings = data_types["StringType"]

missing_data_fill = {}
for var in strings:
  missing_data_fill[var] = "missing"
df2 = df2.fillna(missing_data_fill)

numericals = data_types["DoubleType"] + data_types["IntegerType"] \
                                      + data_types["LongType"]

# COMMAND ----------

missing_data_fill

# COMMAND ----------

df.dtypes


# COMMAND ----------

df.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

numericals = data_types["DoubleType"] + data_types["IntegerType"] \
                                      + data_types["LongType"]

mean_dict = { col: 'mean' for col in numericals}
col_avgs = df2.agg( mean_dict ).collect()[0].asDict()
col_avgs = { k[4:-1]: v for k,v in col_avgs.items() }
df2 = df2.fillna(col_avgs)

# COMMAND ----------

col_avgs

# COMMAND ----------

variable_list_emblem 

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

strings = [var for var in df2.columns if var in data_types["StringType"]]
stage_string = [StringIndexer(inputCol= c, outputCol= c+"_string_encoded") for c in strings]
stage_one_hot = [OneHotEncoder(inputCol= c+"_string_encoded", outputCol= c+ "_one_hot") for c in strings]

ppl = Pipeline(stages= stage_string + stage_one_hot)
df2 = ppl.fit(df2).transform(df2)

from pyspark.ml.feature import Normalizer, VectorAssembler, StandardScaler

numericals = [var for var in df2.columns if var not in data_types["StringType"]]
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
