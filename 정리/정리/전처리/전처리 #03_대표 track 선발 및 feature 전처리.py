# Databricks notebook source
# MAGIC %md
# MAGIC #### Goal 
# MAGIC * 8월과 9월 데이터로 구분
# MAGIC * track 별 데이터 수 확인 + tmi/Youtube/커뮤니티 이용해서 최종 테스트용 트랙 선정 : 빌리지 손가락, 고가의 질주, 차이나 서안 병마용
# MAGIC * actualdistance 변수 생성 

# COMMAND ----------

#statistical test : histogram
test = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/augtooctdata.csv"))


test.registerTempTable("testsql")

# COMMAND ----------

# MAGIC %sql
# MAGIC select track, count(distinct matchId) 
# MAGIC from testsql
# MAGIC where startTime_month = 8 or DATE_FORMAT(startTime,'y-M-d') = '2021-9-1'
# MAGIC group by track;

# COMMAND ----------

# MAGIC %sql
# MAGIC select track, count(distinct matchId) 
# MAGIC from testsql
# MAGIC where startTime_month = 8
# MAGIC group by track;

# COMMAND ----------

# MAGIC %sql
# MAGIC select track, count(distinct matchId) 
# MAGIC from testsql
# MAGIC where DATE_FORMAT(startTime,'y-M-d') = '2021-9-1'
# MAGIC group by track;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 데이터 분류 
# MAGIC 8월 데이터 (2021-08-01 오전 9시 ~ 2021-09-01 오전 9시) </br>
# MAGIC 9월 데이터 (2021-09-01 오전 9시 ~ 2021-10-01 오전 9시)

# COMMAND ----------

###8월 데이터 분류 : 2021-08-01:9am ~ 2021-09-00:9am
august = spark.sql("select  *  from testsql where DATE_TRUNC('second', startTime::timestamp) < '2021-09-01T09:00:00.000+0000'")
august.createOrReplaceTempView("august")

# COMMAND ----------

august.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from august;

# COMMAND ----------

# MAGIC %sql
# MAGIC select  DATE_TRUNC('second', startTime::timestamp)  from testsql where DATE_TRUNC('second', startTime::timestamp) < '2021-09-01T09:00:00.000+0000' limit 3;

# COMMAND ----------

###9월 데이터 분류 : 2021-08-01:9am ~ 2021-09-00:9am
september = spark.sql("select  *  from testsql where DATE_TRUNC('second', startTime::timestamp) >= '2021-09-01T09:00:00.000+0000'")
september.createOrReplaceTempView("september")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from september;

# COMMAND ----------

#데이터프레임 csv로도 저장
import pyspark
from pyspark.sql.functions import *

# august.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/august.csv")
# september.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/september.csv")

# COMMAND ----------

import numpy as np
import pandas as pd
from urllib.request import urlopen
import json

import plotly.io as pio 
import plotly.express as px #빠르게 사용
import plotly.graph_objects as go  #디테일하게 설정해야할때
import plotly.figure_factory as ff
from plotly.subplots import make_subplots # 여러 subplot을 그릴때 
from plotly.validators.scatter.marker import SymbolValidator # 마커사용

# COMMAND ----------

pio.templates

# COMMAND ----------

pio.templates.default = "plotly"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### track별로 구분

# COMMAND ----------

tmpaug = spark.sql("select track, count(distinct matchId) as cnt from august group by track order by count(distinct matchId)").select("*").toPandas()

# COMMAND ----------

import plotly.express as px
tmpaug2 = spark.sql("select track, count(*) as total, count(distinct matchId) as match from august group by track order by 3 desc").select("*").toPandas()
fig = px.bar(tmpaug2, x='track', y='match')
fig.show()

# COMMAND ----------

import plotly.express as px
tmpsep = spark.sql("select track, count(distinct matchId) as cnt from september group by track order by 2 desc").select("*").toPandas()
fig = px.bar(tmpsep, x='track', y='cnt')
fig.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(distinct matchId) as cnt from august group by track order by count(distinct matchId)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, startTime from testsql where track = '빌리지 고가의 질주' order by 2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, startTime from august where track = '빌리지 고가의 질주' order by 2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august where track = '빌리지 고가의 질주' group by track;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august group by track order by 3 desc;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, difficulty from september; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, difficulty from september where track = 코리아 제주 해오름 다운힐; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from september group by track order by 3 desc;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august group by track order by 3 desc;

# COMMAND ----------

# MAGIC %md
# MAGIC 노르테유 익스프레스

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august group by track having track = '노르테유 익스프레스';

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from september group by track having track = '노르테유 익스프레스';

# COMMAND ----------

# MAGIC %md 
# MAGIC 빌리지 시계탑 cnt

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august group by track having track = '빌리지 시계탑';

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from september group by track having track = '빌리지 시계탑';

# COMMAND ----------

차이나 서안 병마용

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august group by track having track = '차이나 서안 병마용';

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from september group by track having track = '차이나 서안 병마용';

# COMMAND ----------

# MAGIC %md
# MAGIC #### 새로운 feature 추가 : actual distance = distance * lap

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august.csv"))


august.registerTempTable("august")

# COMMAND ----------

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september.csv"))


september.registerTempTable("september")

# COMMAND ----------

print(august.groupBy('lap').count().show())
print(august.groupBy('distance').count().show())
print(september.groupBy('lap').count().show())
print(september.groupBy('distance').count().show())

# COMMAND ----------

from pyspark.sql.functions import when
import pyspark.sql.functions as f
august = august.withColumn('lap2', f.when(
        f.length(f.col('lap').substr(1, 3)) == 3,
        f.col('lap').substr(1, 1)
    ).otherwise(f.lit(None)))
august.display()

# COMMAND ----------

september = september.withColumn('lap2', f.when(
        f.length(f.col('lap').substr(1, 3)) == 3,
        f.col('lap').substr(1, 1)
    ).otherwise(f.lit(None)))
september.display()

# COMMAND ----------

#null 값 확인 
print(august.groupBy('lap2').count().display())
print(september.groupBy('lap2').count().display())

# COMMAND ----------

from pyspark.sql.functions import substring, length, col, expr
august.withColumn("distance2",expr("substring(distance, 1, length(distance)-2)")).display()

# COMMAND ----------

august = august.withColumn("distance2",expr("substring(distance, 1, length(distance)-2)"))
september = september.withColumn("distance2",expr("substring(distance, 1, length(distance)-2)"))

# COMMAND ----------

##타입 변경
from pyspark import SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import when

# COMMAND ----------

august.withColumn("distance2",col("distance2").cast("double")).display()

# COMMAND ----------

august = august.withColumn("lap2",col("lap2").cast("float"))
august = august.withColumn("distance2",col("distance2").cast("float"))
september = september.withColumn("lap2",col("lap2").cast("float"))
september = september.withColumn("distance2",col("distance2").cast("float"))

# COMMAND ----------

august.withColumn("actualdistance",col("lap2")*col("distance2")).display()

# COMMAND ----------

august = august.withColumn("actualdistance",col("lap2")*col("distance2"))
september = september.withColumn("actualdistance",col("lap2")*col("distance2"))

# COMMAND ----------

august.display()

# COMMAND ----------

september.display()

# COMMAND ----------

august.registerTempTable("august")
september.registerTempTable("september")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from august;

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(startTime), max(startTime) from september;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from august limit 3;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from september limit 3;

# COMMAND ----------

august.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/august2.csv")
september.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/september2.csv")

# COMMAND ----------

import pyspark.sql.functions as F
august2 = august
print(august2.where(F.col('trackId').isNull()).count())
print(august2.where(F.col('track').isNull()).count())

# COMMAND ----------

september2 = september
print(september2.where(F.col('trackId').isNull()).count())
print(september2.where(F.col('track').isNull()).count())

# COMMAND ----------

august2 = august2.filter(august2.trackId. isNotNull())

# COMMAND ----------

august2 = august2.filter(august2.track. isNotNull())

# COMMAND ----------

august2.registerTempTable("august2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select track, count(*) as total, count(distinct matchId) as match from august2 group by track order by 3 desc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### trackId null 값 제거 및 다시 저장 

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august2.csv"))

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september2.csv"))


august.registerTempTable("august")
september.registerTempTable("september")

# COMMAND ----------

august = august.filter(august.trackId. isNotNull())
september = september.filter(september.trackId. isNotNull())

# COMMAND ----------

print(august.where(F.col('trackId').isNull()).count())
print(september.where(F.col('trackId').isNull()).count())

# COMMAND ----------

august.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/august3.csv")
september.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/september3.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### august 와 september 데이터에 존재하는 잘 못 호출된 데이터 drop 

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august3.csv"))

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september3.csv"))


august.registerTempTable("august")
september.registerTempTable("september")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from august where channelName like '%Newbie' and p_rankinggrade2 > 3; 
# MAGIC -- 총 806개

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from september where channelName like '%Newbie' and p_rankinggrade2 > 3; 
# MAGIC -- 총 465개

# COMMAND ----------

tmp_aug = spark.sql("select * from august where channelName like '%Newbie' and p_rankinggrade2 > 3")
tmp_sep = spark.sql("select * from september where channelName like '%Newbie' and p_rankinggrade2 > 3")

# COMMAND ----------

tmp1 = august.exceptAll(tmp_aug)
tmp1.registerTempTable("tmp1")

# COMMAND ----------

tmp2 = september.exceptAll(tmp_sep)
tmp2.registerTempTable("tmp2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tmp1 where channelName like '%Newbie' and p_rankinggrade2 > 3

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tmp2 where channelName like '%Newbie' and p_rankinggrade2 > 3

# COMMAND ----------

print(august.count())
print(tmp1.count())
print(september.count())
print(tmp2.count())

# COMMAND ----------

11815622 - 11814816

# COMMAND ----------

8698445 -8697980

# COMMAND ----------

tmp1.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/august4.csv")
tmp2.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/september4.csv")