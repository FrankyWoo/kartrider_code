# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/week1_matchinfo_df.csv"))
df.display() 

# COMMAND ----------

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
#df3 = df3.withColumn('startTime_dayofweek', lit(dayofweek(df3.startTime))) #Sunday = 1, Sat = 7 
#df3 = df3.withColumn('startTime_dayofweek', lit(date_format(dayofweek(df3.startTime), "EEE")))
df3 = df3.withColumn('startTime_month', lit(month(df3.startTime)))
df3 = df3.withColumn('startTime_day', lit(dayofmonth(df3.startTime)))
df3 = df3.withColumn('startTime_holiday', when((df3.startTime_dayofweek == 1) | (df3.startTime_dayofweek == 7), lit(1)).otherwise(lit(0)))
df3.display()



# COMMAND ----------

#df3 = df3.drop('p_license')
columns_to_drop = ['Unnamed: 0', 'p_license']
df3 = df3.drop(*columns_to_drop)
df3.display()

# COMMAND ----------

df3=df3.withColumn("p_matchTime", df3.p_matchTime/1000)
df3.display()

# COMMAND ----------

print((df3.count(), len(df3.columns)))
distinctDF = df3.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)


# COMMAND ----------

#df3.select(sorted(df3.columns)).display()
df3 = df3.select('_c0','p_accountNo','startTime','matchId','playTime','channelName','trackId','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day','startTime_holiday') #같이 이야기 해보기. ★요고 먼저 해서 정리부터 하기. 

# COMMAND ----------

df3 = df3.select('_c0','p_accountNo','startTime','matchId','playTime','channelName','trackId','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day') #같이 이야기 해보기. ★요고 먼저 해서 정리부터 하기. 

# COMMAND ----------

df3.display()

# COMMAND ----------

#table로 저장
df3.registerTempTable("df_tmp")

# COMMAND ----------

sqlContext.sql("select * from df_tmp").display()

# COMMAND ----------

sqlContext.sql("select * from df_tmp").display()

# COMMAND ----------

sqlContext.sql("select matchId, teamId, p_matchWin, matchResult from df_tmp").display()

# COMMAND ----------

sqlContext.sql("select *  from df_tmp order by startTime_day").display()

# COMMAND ----------

sqlContext.sql("select *  from df_tmp order by startTime_day desc").display()

# COMMAND ----------

#8 월이 아닌 데이터 확인. 
sqlContext.sql("select * from df_tmp where startTime_month != 8").display

# COMMAND ----------

df3.registerTempTable("df_tmp")

# COMMAND ----------

df3 = df3.filter(df3.startTime_month == 8)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp").display()

# COMMAND ----------

# 8월 제거 
sqlContext.sql("select * from df_tmp where startTime_month != 8").display()

# COMMAND ----------

#킹그레이드별 리타이어율
#화수목/금토일 나눠서 유저 파악(유저수와 유저정보)
#1간단위 나눠서 유저수파악[+리타이어비율]

# COMMAND ----------

df4 = df3.drop('_c0')

# COMMAND ----------

df4.registerTempTable("df_tmp2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select p_rankinggrade2, count(p_rankinggrade2), (sum(p_matchRetired)/count(p_rankinggrade2)) 
# MAGIC from df_tmp2
# MAGIC group by p_rankinggrade2 
# MAGIC order by p_rankinggrade2

# COMMAND ----------

# MAGIC %md
# MAGIC 리타이어시 1, 아닐시 0 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_dayofweek, count(startTime_dayofweek)
# MAGIC from df_tmp2
# MAGIC group by startTime_dayofweek
# MAGIC order by startTime_dayofweek

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_hour, count(startTime_hour) as usercnt, (sum(p_matchRetired)/count(startTime_hour)) as retireduserrate
# MAGIC from df_tmp2
# MAGIC group by startTime_hour
# MAGIC order by startTime_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(p_rankinggrade2), (sum(p_matchRetired)/count(p_rankinggrade2)), count(p_rankinggrade2) 
# MAGIC from df_tmp2
# MAGIC where p_rankinggrade2 == 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(p_rankinggrade2), (sum(p_matchRetired)/count(p_rankinggrade2)), count(p_rankinggrade2) 
# MAGIC from df_tmp2
# MAGIC where p_rankinggrade2 == 2

# COMMAND ----------

# 분포 조회 
# 랭킹그레이드별 리타이어율
# 월화수목/금토일 나눠서 유저 파악(유저수와 유저정보)
# 1시간단위 나눠서 유저수파악[+리타이어비율]