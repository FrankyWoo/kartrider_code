# Databricks notebook source
august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august7.csv"))

august.registerTempTable("august")

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september6.csv"))

september.registerTempTable("september")

# COMMAND ----------

august

# COMMAND ----------

df = august.union(september)
df.registerTempTable("df")

# COMMAND ----------

### types
df.dtypes

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



# COMMAND ----------

df2.display()

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

numericals = data_types["DoubleType"] + data_types["IntegerType"] \
                                      + data_types["LongType"]

mean_dict = { col: 'mean' for col in numericals}
col_avgs = df2.agg( mean_dict ).collect()[0].asDict()
col_avgs = { k[4:-1]: v for k,v in col_avgs.items() }
df2 = df2.fillna(col_avgs)

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

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.registerTempTable("df2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df2 where track = '고가의 질주'

# COMMAND ----------

#https://people.stat.sc.edu/haigang/GLM_in_spark.html