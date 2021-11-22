# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
df = spark.read.csv('/FileStore/KartRider/week1_matchinfo_df.csv', header="true", inferSchema="true")

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *

#spark.conf.set("spark.sql.session.timeZone", "ROK") => 전체 적용 예상됨.. 일단 보류
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/week1_matchinfo_df.csv"))
df.display() 



# COMMAND ----------

# MAGIC %md
# MAGIC * drop : Unnamed: 0, matchType
# MAGIC * lambda : metadata 변환
# MAGIC * 날짜 : 1. 날짜 변환 -> Seoul 2. 요일 변수 3. 시간 변수 4. 공휴일/평일 변수

# COMMAND ----------

# type
df.dtypes #startTime & endTime -> string

# COMMAND ----------

# UTC to ROK
df3 = df.withColumn('startTime',from_utc_timestamp("startTime", "ROK"))
df3 = df3.withColumn('endTime', from_utc_timestamp("endTime", "ROK"))

# 시작 시간 기준으로 시간, 분 컬럼 생성 -> ★토의하기 
df3 = df3.withColumn('startTime_hour', lit(hour(df3.startTime)))
df3 = df3.withColumn('startTime_minute', lit(minute(df3.startTime)))
df3 = df3.withColumn('startTime_second', lit(second(df3.startTime)))
df3 = df3.withColumn('startTime_quarter', lit(quarter(df3.startTime)))
df3 = df3.withColumn('startTime_dayofweek', lit(dayofweek(df3.startTime))) #Sunday = 1, Sat = 7
df3.display()


# COMMAND ----------

print((df3.count(), len(df3.columns)))

# COMMAND ----------

distinctDF = df3.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

# COMMAND ----------

print(df3.columns)
['_c0', 'Unnamed: 0', 'channelName', 'endTime', 'gameSpeed', 'matchId', 'matchResult', 'matchType', 'playTime', 'startTime', 'trackId', 'teamId', 'teamPlayers', 'p_characterName', 'p_character', 'p_kart', 'p_license', 'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit', 'p_rankinggrade2', 'p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime', 'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek']

# COMMAND ----------

df3.select(sorted(df3.columns)).display()
df3.select('_c0','p_accountNo','') #같이 이야기 해보기. ★요고 먼저 해서 정리부터 하기. 

# COMMAND ----------

#columns_to_drop = ['id', 'id_copy']
#df = df.drop(*columns_to_drop)
df3 = df3.drop('p_license')
df3.display()

# COMMAND ----------

df3=df3.withColumn("p_matchTime", df3.p_matchTime/1000)
df3.show()



# COMMAND ----------

df3.display()

# COMMAND ----------

#to table. 
df.registerTempTable("df_tmp")
df_
df2 = sqlContext.sql("select * from df_tmp")
df2_pd = df2.toPandas()


# COMMAND ----------

sqlContext.sql("select * from df_tmp").display()
df3 = 

# COMMAND ----------

# 시간 , 분 따로 컬럼 추가
df3 = df3.withColumn('startTime_hour', lit(hour(df3.startTime)))
df3 = df3.withColumn('startTime_hour', lit(minute(df3.startTime)))
df3.display()



# from datetime import timedelta
# df3 = df3.withColumn('startTime_hour',lit(hour('startTime')))

# df3 = df3.withColumn('startTime_hour', df3.startTime_hour + timedelta(hours=9))
#df3 = df3.withColumn('startTime_hour',from_utc_timestamp('startTime_hour','ROK'))
# df3 = df3.withColumn('startTime_min', lit(minute("startTime")) 

# 다시 +9 시간 하기 
# df3 = df.withColumn('startTime_hour',from_utc_timestamp("startTime_hour", "ROK"))
# df3 = df.withColumn('startTime_min', from_utc_timestamp("startTime_min", "ROK"))


# COMMAND ----------

df.display()

# COMMAND ----------

df = (empdf
    .select("date")
    .withColumn("pst_timestamp", from_utc_timestamp("date", "PST")))
df.show(2)

# COMMAND ----------

emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])

# COMMAND ----------

from pyspark.sql.functions import *
 
example = spark.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
example.select(to_utc_timestamp(example.ts, "PST").alias('utc_time')).collect()
[Row(utc_time=datetime.datetime(1997, 2, 28, 18, 30))]
example.select(to_utc_timestamp(example.ts, example.tz).alias('utc_time')).collect()
[Row(utc_time=datetime.datetime(1997, 2, 28, 1, 30))]

# COMMAND ----------

df.show()

# COMMAND ----------

#df.select('var_0').describe().show()
df.describe().show()