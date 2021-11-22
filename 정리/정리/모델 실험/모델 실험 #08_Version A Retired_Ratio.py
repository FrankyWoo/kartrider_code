# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/random_selected.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC type 변경

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("p_matchRetired",col("p_matchRetired").cast(StringType()))

# COMMAND ----------

#import pyspark.sql.functions as func
# df2 = df2.withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
#     .withColumn("difficulty", func.round(df["difficulty"]).cast('integer'))

df2 = df2.withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))

# COMMAND ----------

### 코드 적용 
df2.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Stats 사용을 위해 Pandas 변환 

# COMMAND ----------

panda = df2.toPandas()

# COMMAND ----------

panda

# COMMAND ----------

del panda['rs']
del panda['trackId']

# COMMAND ----------

## type 확인
import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Label Encoding 

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

panda

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 전처리 문제 발생 확인. 해결하쟈 

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august7.csv"))

august.registerTempTable("august")

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september6.csv"))

september.registerTempTable("september")

df = august.union(september)
df.registerTempTable("df")

# COMMAND ----------

columns_to_drop = ['retired_ratio']
df = df.drop(*columns_to_drop)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df where track = '쥐라기 아슬아슬 화산 점프'

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august.registerTempTable("august")

# COMMAND ----------

august6 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august6.csv"))

august6.registerTempTable("august6")

september5 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september5.csv"))

september5.registerTempTable("september5")

df = august6.union(september5)
df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRetired, count(*) from df  group by p_matchRetired having p_matchRetired = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRetired, count(*) from df  group by p_matchRetired having p_matchRetired = 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRetired, count(*) from august6  group by p_matchRetired having p_matchRetired = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRetired, count(*) from september5  group by p_matchRetired having p_matchRetired = 1

# COMMAND ----------

august4 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv"))

august4.registerTempTable("august4")

# COMMAND ----------

september4 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september4.csv"))

september4.registerTempTable("september4")

# COMMAND ----------

aa = august4.union(september4)
aa.registerTempTable("aa")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRetired, count(*) from aa  group by p_matchRetired having p_matchRetired = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_matchRetired, count(*) from aa  group by p_matchRetired having p_matchRetired = 0

# COMMAND ----------

file = spark.sql("select track, sum(p_matchRetired) as ss, count(track) as cc, (sum(p_matchRetired)/count(p_matchRetired)) as retiredRatio from aa group by track")

# COMMAND ----------

file.display()

# COMMAND ----------

2408/9896

# COMMAND ----------

2086/2650

# COMMAND ----------

file.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/retiredratiotable.csv")

# COMMAND ----------

file.registerTempTable("file")

# COMMAND ----------

columns_to_drop = ['retired_ratio']
final = final.drop(*columns_to_drop)

# COMMAND ----------

final = spark.sql("""
               select a.*, b.retiredRatio 
               from df as a 
               left join file as b 
               on 1=1
               and a.track = b.track""")

# COMMAND ----------

final.display()

# COMMAND ----------

final.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versionA.csv")

# COMMAND ----------

final.count()

# COMMAND ----------

1251609

# COMMAND ----------

df4 = spark.sql("""
select a.*, b.name
from df_tmp as a
left join kart_meta as b
on 1=1
and a.p_kart = b.id
""")

df4.registerTempTable("df4")