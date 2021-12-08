# Databricks notebook source
# 파일 오픈 방법 

import pyspark
from pyspark.sql.functions import *
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df.csv"))
#df.display() 
#df.display()  # df 형태로 보려면 항상 변수명 뒤에 .display() 붙여야함!
# 파일 저장 : df3.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/파일명.csv")

# COMMAND ----------

df.display()
df.dtypes

# COMMAND ----------

# 시간 변경 UTC -> ROK(한국 시간)
## UTC to ROK
df3 = df.withColumn('startTime',from_utc_timestamp("startTime", "ROK"))
df3 = df3.withColumn('endTime', from_utc_timestamp("endTime", "ROK"))



# 시작 시간 기준으로 시간, 분 컬럼 생성
## spark : 
##1) .withColumn : df의 컬럼을 이용해서 하는 대부분의 경우 사용 
##2) lit : 새로운 컬럼 생성
##3) when (여기 셀 마지막 부분봐봐! ) / otherwise: if-else 의 기능
df3 = df3.withColumn('startTime_hour', lit(hour(df3.startTime)))
df3 = df3.withColumn('startTime_minute', lit(minute(df3.startTime)))
df3 = df3.withColumn('startTime_second', lit(second(df3.startTime)))
df3 = df3.withColumn('startTime_quarter', lit(quarter(df3.startTime)))
df3 = df3.withColumn('startTime_dayofweek', lit(dayofweek(df3.startTime))) #Sunday = 1, Sat = 7 
#df3 = df3.withColumn('startTime_dayofweek', lit(date_format(dayofweek(df3.startTime), "EEE")))
df3 = df3.withColumn('startTime_month', lit(month(df3.startTime)))
df3 = df3.withColumn('startTime_day', lit(dayofmonth(df3.startTime)))
df3 = df3.withColumn('startTime_holiday', when((df3.startTime_dayofweek == 1) | (df3.startTime_dayofweek == 7), lit(1)).otherwise(lit(0)))
df3 = df3.withColumn('name_dayofweek', when((df3.startTime_dayofweek == 2), lit("Mon"))
                     .when((df3.startTime_dayofweek == 3), lit("Tue"))\
                     .when((df3.startTime_dayofweek == 4), lit("Wed"))\
                     .when((df3.startTime_dayofweek == 5), lit("Thu"))\
                     .when((df3.startTime_dayofweek == 6), lit("Fri"))\
                     .when((df3.startTime_dayofweek == 7), lit("Sat"))\
                     .otherwise("Sun")
  
  )


# 만일, default 시간 변경 원할 경우
#spark.conf.set("spark.sql.session.timeZone", "ROK")

# from pyspark.sql.functions import when
# df.withColumn("grade", \
#    when((df.salary < 4000), lit("A")) \
#      .when((df.salary >= 4000) & (df.salary <= 5000), lit("B")) \
#      .otherwise(lit("C")) \
#   ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 간단한 전처리 !

# COMMAND ----------

# 필요없는 컬럼 제거 
## 단일 컬럼일 경우
#df3 = df3.drop('p_license') 
# 여러 컬럼인 경우 
columns_to_drop = ['Unnamed: 0', 'p_license', '_c0'] 
df3 = df3.drop(*columns_to_drop)
df3.display()


#matchTime 시간 
df3=df3.withColumn("p_matchTime", df3.p_matchTime/1000)
df3.display()



#반복된 데이터 확인 => 없음을 확인
print((df3.count(), len(df3.columns)))
distinctDF = df3.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)


#컬럼 순서 변경
df3 = df3.select('p_accountNo','startTime','matchId','playTime','channelName','trackId','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day','startTime_holiday','name_dayofweek') 




# COMMAND ----------

# sql 임시 테이블로 저장 (view로 저장)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp").display()

# COMMAND ----------


sqlContext.sql("select matchId, teamId, p_matchWin, matchResult from df_tmp").display()
# 날짜 데이터 정보 확인하던 중, 5월 데이터 확인. 
#8 월이 아닌 데이터 확인. => 38개
sqlContext.sql("select * from df_tmp where startTime_month != 8").display()
#8월이 아닌 데이터 정보 지우기 
df3 = df3.filter(df3.startTime_month == 8)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### spark 상태에서 바로 sql!</br>
# MAGIC spark.sql("select * from df_tmp").display()

# COMMAND ----------

# 1차 가공한 데이터 저장 
#df3.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/파일명") 
df3.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/week1_matchinfo_df3_cy.csv")  #week1_matchinfo_df2로 저장해둔 파일이 있음 

# COMMAND ----------

df4 = df3.drop('_c0')
df4.registerTempTable("df_tmp2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_tmp2

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select p_rankinggrade2, count(p_rankinggrade2), (sum(p_matchRetired)/count(p_rankinggrade2)) as retireduserrate
# MAGIC from df_tmp2
# MAGIC group by p_rankinggrade2 
# MAGIC order by p_rankinggrade2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_dayofweek, name_dayofweek
# MAGIC from df_tmp2

# COMMAND ----------

# MAGIC %sql 
# MAGIC  
# MAGIC select name_dayofweek, count(name_dayofweek)
# MAGIC from df_tmp2
# MAGIC group by name_dayofweek
# MAGIC order by name_dayofweek

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_hour, count(startTime_hour) as usercnt, (sum(p_matchRetired)/count(startTime_hour)) as retireduserrate
# MAGIC from df_tmp2
# MAGIC group by startTime_hour
# MAGIC order by startTime_hour

# COMMAND ----------

spark.sql("select * from df_tmp2").display()

# COMMAND ----------

channelinfo = spark.sql("select channelName, count(channelName) as cnt, (sum(p_matchRetired)/count(p_matchRetired)) as retired_ratio from df_tmp2 group by channelName")
channelinfo.display()

# COMMAND ----------

#sql table을 pandas로 변환 ! df기능 사용하려면 무조건 해야함
channelinfo = channelinfo.select("*").toPandas() 

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

plt.figure(figsize=(15, 6))
#sns.barplot(x="cnt", hue="channelName", y="retired_ratio", data=channelinfo)
sns.barplot(x="channelName", y="cnt",data=channelinfo)
plt.show()

# COMMAND ----------

import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns
# plt.figure(figsize=(15, 6))
 
# sns.barplot(x="channelName", y="retired_ratio", data=channelinfo)
# plt.show()
 
 
f, ax = plt.subplots(1,1,figsize=(16,8))
plt.bar(channelinfo.channelName, channelinfo.retired_ratio, color='green', label='target')
plt.bar(channelinfo.channelName, channelinfo.cnt, color='blue', label='control')
plt.xlabel("date")
plt.ylabel("playtime")
plt.title("date and playtime")
plt.legend()
plt.show()

# COMMAND ----------

channel2 = spark.sql("select channelName, count(channelName) as cnt, name_dayofweek from df_tmp2 group by channelName, name_dayofweek order by channelName")
channel2 = channel2.select("*").toPandas()
channel2.display()

# COMMAND ----------

# 채널별, 채널별 * 등급별. 
plt.figure(figsize=(15, 8))
#sns.barplot(x="cnt", hue="channelName", y="retired_ratio", data=channelinfo)
sns.barplot(x="name_dayofweek", hue ="channelName", y="cnt",data=channel2)
plt.show()

# COMMAND ----------

파 :  빠름
노: 초보 채널, 없음 따로 찾아봐야할듯. 
초 : 매우 빠름
빨 : 무한 부스터, 빠름
보 : 초보 채널, 보통
브 : 매우 빠름. 
  
newbie 자체가 약간 신생아 클럽의 느낌 있음.
신규 유저 유입률을 낮을 것으로 예측됨. => 상관관계 결과 뜨는 데로 다시 확인.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 메타데이터 병합

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *
# 가공한 데이터 파일 오픈. 
df4= (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df4.csv"))

df4.registerTempTable("df4")
#df.display() 

# COMMAND ----------

kart_meta = spark.read.json("/FileStore/KartRider/2/3/kart.json")
kart_meta.registerTempTable("kart_meta")

# COMMAND ----------



# COMMAND ----------

track_meta = spark.read.json("/FileStore/KartRider/2/3/track_detail.json")
track_meta.registerTempTable("track_meta")
track_info=spark.read.json("/FileStore/KartRider/track.json")
track_info.registerTempTable("track_info")

# COMMAND ----------

df5 = spark.sql("""
select a.*, b.name
from df4 as a
left join track_meta as b
on 1=1
and a.p_kart = b.id
""")

df4.registerTempTable("df4")
df4.display()

# COMMAND ----------

track_table=spark.sql("""
select a.*, b.*
from track_info as a
left join track_meta as b
on 1=1
and a.name = b.track
""")

# COMMAND ----------

track_table=track_table.drop(track_table._corrupt_record)

# COMMAND ----------

track_table.display()

# COMMAND ----------

track_table.registerTempTable("track_tmp")

# COMMAND ----------

track_df=spark.sql("""
select id, track, difficulty, distance, lap, mode from track_tmp
""")

# COMMAND ----------

track_df.registerTempTable("track_df")

# COMMAND ----------

final_df=spark.sql("""
select *
from df4 as a
left join track_df as b
on 1=1
and a.trackname= b.track
""")

final_df.registerTempTable("final_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_df

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_df where difficulty is not null

# COMMAND ----------

final_df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/2/3/final_df.csv")

# COMMAND ----------

re

# COMMAND ----------


ranking_difficulty=spark.sql("""
select p_rankinggrade2,difficulty,count(difficulty) as difficulty_cnt 
from  final_df
where difficulty is not null 
group by p_rankinggrade2,difficulty
order by p_rankinggrade2, difficulty
""")

# COMMAND ----------

ranking_difficulty.select("*").toPandas

# COMMAND ----------

final_df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv"))

# COMMAND ----------

final_df.registerTempTable("final_df")
final_df.select("*").toPandas

# COMMAND ----------

# import matplotlib.pyplot as plt

fianl_df.select("difficulty").show()


# COMMAND ----------

import pandas as pd
ranking_difficult_df=pd.DataFrame([ranking_difficulty.select("difficulty"),ranking_difficulty.select("difficulty_cnt")])

# COMMAND ----------

test = spark.sql("select p_rankinggrade2,difficulty,count(difficulty) as difficulty_cnt from final_df where difficulty is not null group by p_rankinggrade2,difficulty order by p_rankinggrade2, difficulty ")
tt = test.select("*").toPandas()
ttt = pd.DataFrame(tt)
copy = ttt.pivot(index ='p_rankinggrade2', columns = 'difficulty', values = 'difficulty_cnt')
copy.display()

# COMMAND ----------

# 채널별 리뉴얼 라이센스
# Dodged Bar Chart by pandas
plt.figure(figsize=(16, 8))
copy.plot(kind='bar')
plt.title('Difficulty - License', fontsize=20)
#plt.legend(fontsize='12')
#plt.figure(figsize=(16, 8))
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(playTime),playTime from final_df where trackname='빌리지 고가의 질주' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select channelName.uni from final_df