# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
# 가공한 데이터 파일 오픈. 
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df.csv"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.dtypes

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


# COMMAND ----------

df3.columns

# COMMAND ----------

# 필요없는 컬럼 제거 
#df3 = df3.drop('p_license') # 단일 컬럼일 경우
columns_to_drop = ['Unnamed: 0', 'p_license', '_c0']
df3 = df3.drop(*columns_to_drop)
df3.display()

# COMMAND ----------

#matchTime 시간 
df3=df3.withColumn("p_matchTime", df3.p_matchTime/1000)
df3.display()

# COMMAND ----------

#반복된 데이터 확인 => 없음을 확인
print((df3.count(), len(df3.columns)))
distinctDF = df3.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

# COMMAND ----------

#컬럼 순서 변경
df3 = df3.select('p_accountNo','startTime','matchId','playTime','channelName','trackId','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day','startTime_holiday','name_dayofweek') 

# COMMAND ----------

# sql 임시 테이블로 저장 (view 저장)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp").display()

# COMMAND ----------

# 날짜 데이터 정보 확인하던 중, 5월 데이터 확인. 
sqlContext.sql("select *  from df_tmp order by startTime_day desc").display()

# COMMAND ----------

#8 월이 아닌 데이터 확인. => 68개
sqlContext.sql("select * from df_tmp where startTime_month != 8").display()

# COMMAND ----------

#8월이 아닌 데이터 정보 지우기 
df3 = df3.filter(df3.startTime_month == 8)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp where startTime_month != 8").display()

# COMMAND ----------

# 1차 가공한 데이터 저장 
df3.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df2.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df2.csv"))

df.registerTempTable("df_tmp")

# COMMAND ----------

kart_meta = spark.read.json("/FileStore/KartRider/2/3/kart.json")
kart_meta.registerTempTable("kart_meta")
df2 = spark.sql("""
select a.*, b.name as kartname
from df_tmp as a
left join kart_meta as b
on 1=1
and a.p_kart = b.id
""")

df2.registerTempTable("df_tmp")
df2.display()

# COMMAND ----------

#컬럼 순서 변경
df2 = df2.select('p_accountNo','startTime','matchId','playTime','channelName','trackId','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','kartname','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day','startTime_holiday','name_dayofweek') 

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.registerTempTable("df_tmp")

# COMMAND ----------

track_meta = spark.read.json("/FileStore/KartRider/2/3/track.json")
track_meta.registerTempTable("track_meta")
df4 = spark.sql("""
select a.*, b.name as trackname
from df_tmp as a
left join track_meta as b
on 1=1
and a.trackId = b.id
""")

df4.registerTempTable("df_tmp2")
df4.display()


# COMMAND ----------

# MAGIC  %sql
# MAGIC  
# MAGIC  select * from track_meta

# COMMAND ----------

#컬럼 순서 변경
df4 = df4.select('p_accountNo','startTime','matchId','playTime','channelName','trackId','trackname','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','kartname','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day','startTime_holiday','name_dayofweek') 

# COMMAND ----------

df4.display()

# COMMAND ----------

# 2차 가공한 데이터 저장 
df4.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df4.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ----------------------eda! ---------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

df5 = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/matchinfo_0801_to_0817_df4.csv"))

df5.registerTempTable("df_tmp")

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *

spark.sql("select * from df_tmp").display()

# COMMAND ----------

test = spark.sql("select channelName, p_rankinggrade2, count(p_rankinggrade2) as rankinggrade2count from df_tmp group by channelName, p_rankinggrade2 order by channelName, p_rankinggrade2 ")

# COMMAND ----------

test.display()

# COMMAND ----------

test.select('channelName').show()
#df.select('zip_code').collect()

# COMMAND ----------

tt = test.select("*").toPandas()

# COMMAND ----------

#making a DataFrame with 'channel' and 'p_rankinggrade2'
import pandas as pd

ttt = pd.DataFrame(tt.groupby(['channelName', 'p_rankinggrade2']).rankinggrade2count.sum())
ttt = ttt.reset_index()

# COMMAND ----------

ttt.display()

# COMMAND ----------

copy = ttt.pivot(index ='channelName', columns = 'p_rankinggrade2', values = 'rankinggrade2count')
copy.display()

# COMMAND ----------

ttt = ttt.pivot(index = 'p_rankinggrade2', columns = 'channelName', values = 'p_rankinggrade2')

# COMMAND ----------

# 이 결과 표로... 매칭 시스템 하려면 조금 더 무언가를 초점으로 해야할까??............채널과 랭킹 그레이드를 이어주는 추가적인 한 요소..! (playtime이지 않을까....?)
copy

# COMMAND ----------

copy.var()

# COMMAND ----------

copy[0]

# COMMAND ----------

copy.describe()

# COMMAND ----------

copy[0].histplot

# COMMAND ----------

# fig 크기가 안 커짐.
# Stacked Bar Chart by pandas
import matplotlib.pyplot as plt

copy.plot.bar(stacked=True, rot=0)
plt.title('Stacked Bar Chart', fontsize=20)
plt.figure(figsize=(20, 10))
plt.show()


# COMMAND ----------

# 채널별 리뉴얼 라이센스
# Dodged Bar Chart by pandas
plt.figure(figsize=(16, 8))
copy.plot(kind='bar')
plt.title('Channel - License', fontsize=20)
#plt.legend(fontsize='12')
#plt.figure(figsize=(16, 8))
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC rankinggrade2 : 현재 라이센스. 
# MAGIC 0 : 없음 1~6 : 초보부터 PRO까지 
# MAGIC 게임 종료시 : ""
# MAGIC 
# MAGIC * 가설 : pro 등급은 초보 채널은 이용하려 하지 않을 것이다. 
# MAGIC pro 라이센스가 가장 많이 이용하는 채널 : speedTeamFastest
# MAGIC pro 등급이 가장 이용하지 않는 채널 : Newbie(초보 채널)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_tmp

# COMMAND ----------

#playtime과 채널 비교 (랭킹 그레이드와 playtime 과의 관계와 같이 파악)
rider = spark.sql("select channelName, avg(playtime), min(playtime), (min(playtime)-10) as first from df_tmp group by channelName")
rider.display() #playtime은 1등기록 + 10초 . (즉, -10초는 1등기록) 말도 안되게 짧은 시간은 이후 어뷰징 유저 탐지로 사용하여도 될 듯. 

# COMMAND ----------

#채널별 리타이어 비교 
picasso = spark.sql("select channelName, sum(p_matchRetired)/count(p_matchRetired) as retired_ratio from df_tmp group by channelName")
picasso.display()

# COMMAND ----------

picasso.select("*").toPandas()

# COMMAND ----------

#playtime과 채널 비교 (랭킁그레이드와 playtime 과의 관계와 같이 파악)
spark.sql("select channelName, trackname, playtime from df_tmp ").display()

# COMMAND ----------

#channel별 시간대에 따라 playtime이 다를 것이다. => 각 채널별로 자세히 파악 
spark.sql("select channelName, startTime_hour, playtime from df_tmp").display()

# COMMAND ----------

spark.sql("select channelName from df_tmp group by channelName").display()

# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'speedTeamInfinit'").display()


# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'speedTeamNewbie' order by startTime_hour").display()

# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'grandprix_speedTeamInfinit'").display()

# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'speedTeamFast'").display()

# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'speedTeamFastest'").display()

# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'speedTeamFastNewbie'").display()

# COMMAND ----------

spark.sql("select startTime_hour, playtime from df_tmp where channelName = 'tierMatching_speedTeam'").display()

# COMMAND ----------

F# & or | ?
gg = spark.sql("select channelName, name_dayofweek, count(name_dayofweek) as daycnt from df_tmp where startTime_day != 15 & startTime_day != 16 & startTime_day != 17 group by channelName, name_dayofweek order by channelName, name_dayofweek")
gg.display()

# COMMAND ----------

gg.select("*").toPandas()

# COMMAND ----------

import seaborn as sns
df6 = df5.select("*").toPandas()
result= df6[['channelName','playTime','gameSpeed','p_rankinggrade2','matchResult','p_matchRank','p_matchRetired','p_matchWin','p_matchTime','startTime_hour']].corr(method='pearson')
ax = sns.heatmap(
    result, 
    vmin=-1, vmax=1, center=0,
    cmap=sns.diverging_palette(20, 220, n=200),
    square=True
)
ax.set_xticklabels(
    ax.get_xticklabels(),
    rotation=45,
    horizontalalignment='right'
);

# COMMAND ----------

result.display()

# COMMAND ----------

channelinfo = spark.sql("select channelName, count(channelName) as cnt from df_tmp group by channelName")
channelinfo.display()
channelinfo = channelinfo.select("*").toPandas()


# COMMAND ----------

plt.figure(figsize=(15, 6))
#sns.barplot(x="cnt", hue="channelName", y="retired_ratio", data=channelinfo)
sns.barplot(x="channelName", y="cnt",data=channelinfo)
plt.show()

# COMMAND ----------

#요일별. 일월화수 양이 하루치씩 더 많음. 그걸 고려했을 때, 결과 해석 필요. 그걸 감안해도 일욜이 많음. 
#spark.sql("select * from df_tmp").display()
channel2 = spark.sql("select channelName, count(channelName) as cnt, name_dayofweek from df_tmp where startTime_day != 15 and startTime_day != 16 and startTime_day != 17 group by channelName, name_dayofweek  order by CASE WHEN name_dayofweek = 'Sun' THEN 1 WHEN name_dayofweek = 'Mon' THEN 2 WHEN name_dayofweek = 'Tue' THEN 3 WHEN name_dayofweek = 'Wed' THEN 4 WHEN name_dayofweek = 'Thu' THEN 5 WHEN name_dayofweek = 'Fri' THEN 6 WHEN name_dayofweek = 'Sat' THEN 7 END ASC")
channel2 = channel2.select("*").toPandas()
channel2.display()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
plt.figure(figsize=(20, 7))
plt.legend(loc = 'upper right')
#sns.barplot(x="cnt", hue="channelName", y="retired_ratio", data=channelinfo)
sns.barplot(x="name_dayofweek", hue ="channelName", y="cnt",data=channel2)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 가장 많이 많이한 맵 3개 
# MAGIC 채널 네임별로 평균 플레이 타임 
# MAGIC 랭킹 그레이드별로 플레이 타임. 

# COMMAND ----------

  spark.sql("select trackname, count(trackname) as cnttrackname from df_tmp group by trackname order by count(trackname) desc limit 3").display()

# COMMAND ----------

final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv"))

final.registerTempTable("final_df")

# COMMAND ----------

# 빌리지 운하가 초보자 뿐만 아니라 고인물도 많이 한다. 보여준 후. 지표로 사용. playtime 비교. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_df

# COMMAND ----------

  trackinfo = spark.sql("select trackname, count(trackname) as cnttrackname from final_df group by trackname order by count(trackname) desc limit 3")
  trackinfo.display()

# COMMAND ----------

impor = spark.sql("select trackname, difficulty, distance, lap, mode, p_rankinggrade2, playtime from final_df")
impor.display()
# spark.sql("select (distinct b.trackname) as trackname2, b.cnttrackname, difficulty, distance, lap, mode  from final_df as a left join trackinfo as b on  1=1 and a.trackname = b.trackname group by trackname").display()

# df2 = spark.sql("""
# select a.*, b.name as kartname
# from df_tmp as a
# left join kart_meta as b
# on 1=1
# and a.p_kart = b.id
# """)


# COMMAND ----------

#trackinfo.registerTempTable("trackinfo")
#impor.registerTempTable("impor")
ma = spark.sql("select a.trackname, cnttrackname, difficulty, distance, lap, mode, p_rankinggrade2, playTime from trackinfo as a left join impor as b on 1=1 and a.trackname = b.trackname")
ma.display()

# df = spark.createDataFrame(data, ["customer_id", "name"])
# df.groupBy("name").pivot("customer_id").count().show()
#grouped = spark.createDataFrame(final, ["trackname, difficulty, distance, lap, mode"])
#grouped = final.groupBy("trackname").pivot("p_rankinggrade2").count().columns(['trackname, difficulty, p_rankinggrade2, distance, lap, mode'])

# COMMAND ----------

ma.registerTempTable("ma")
#spark.sql("select * from ma group by p_rankinggrade2, trackname, cnttrackname, difficulty, distance, lap, mode").display()

# COMMAND ----------

# df = spark.createDataFrame(data, ["customer_id", "name"])
# df.groupBy("name").pivot("customer_id").count().show()
#grouped = spark.createDataFrame(final, ["trackname, difficulty, distance, lap, mode"])
ma2 = ma.groupBy("trackname").pivot("p_rankinggrade2").count()
ma2

# COMMAND ----------

ma2.display()

# COMMAND ----------

#랭킹 그레이드별 플레이 타임
spark.sql("select group by p_rankinggrade2")

# COMMAND ----------

impor.display()

# COMMAND ----------

spark.sql("select trackname, playtime, p_rankinggrade2 from final_df where trackname = '빌리지 고가의 질주'").display()

# COMMAND ----------

cream = spark.sql("select trackname, avg(playtime) as avgplaytime, p_rankinggrade2 from final_df where trackname = '빌리지 고가의 질주' group by trackname, p_rankinggrade2 order by p_rankinggrade2")
cream.display()

# COMMAND ----------

b = spark.sql("select trackname, avg(playtime) as avgplaytime, p_rankinggrade2 from final_df where trackname = '코리아 롯데월드 어드벤처' group by trackname, p_rankinggrade2 order by p_rankinggrade2 ")
b.display()

# COMMAND ----------

c = spark.sql("select avg(playtime) as avgplaytime, p_rankinggrade2 from final_df where trackname = '빌리지 운하' group by trackname, p_rankinggrade2 order by p_rankinggrade2")
c.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC rankinggrade 기준으로 win , retired 이용해서 실력 점수 추정.

# COMMAND ----------

#변수 matchwin, retired, rankinggrade는 큰 상관관계를 가질 것이다. 실력 점수 절대 변수로 예상.
# p_matchRetired 는 반비례 관계 예상 
#gamespeed 종류 
spark.sql("select p_accountNo, count(p_accountNo), sum(matchResult), sum(p_matchWin), sum(p_matchRetired) from df_tmp group by p_accountNo").display()


# COMMAND ----------

ss= spark.sql("select p_rankinggrade2,gameSpeed, count(gameSpeed) as speedcount from df_tmp group by p_rankinggrade2, gameSpeed")

# COMMAND ----------

# 채널별 리타이어 비교  
# 채널별 1등기록, 평균 시간......
# 채널별 시간대 분배 
# 가상 : 채널별 실력 기준 정하기. => 

# COMMAND ----------

# MAGIC %sql
# MAGIC select p_rankinggrade2, count(p_rankinggrade2), (sum(p_matchRetired)/count(p_rankinggrade2)) as retireduserrate
# MAGIC from df_tmp
# MAGIC group by p_rankinggrade2 
# MAGIC order by p_rankinggrade2

# COMMAND ----------

# MAGIC %md 
# MAGIC 시간의 경우는 날짜에 맞게 비율 조정이 필요. 
# MAGIC 월 3 화 3 수 2 목 2 금 2 토 2  일 3

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_dayofweek, name_dayofweek
# MAGIC from df_tmp

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select name_dayofweek, count(name_dayofweek)
# MAGIC from df_tmp
# MAGIC group by name_dayofweek
# MAGIC order by name_dayofweek

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_hour, count(startTime_hour) as usercnt, (sum(p_matchRetired)/count(startTime_hour)) as retireduserrate
# MAGIC from df_tmp
# MAGIC group by startTime_hour
# MAGIC order by startTime_hour

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_hour, count(startTime_hour) as usercnt
# MAGIC from df_tmp
# MAGIC group by startTime_hour
# MAGIC order by startTime_hour