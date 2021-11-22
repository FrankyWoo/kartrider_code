# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/week1_matchinfo_df.csv"))
#df.display() 

# COMMAND ----------

df.dtypes

# COMMAND ----------

### MSSQL : convert(date, [datetime], 112)
### Spark-SQL (Hive) : to_date(datetime)
### MySQL   

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


# 만일, default 시간 변경 원할 경우
#spark.conf.set("spark.sql.session.timeZone", "ROK")

# from pyspark.sql.functions import when
# df.withColumn("grade", \
#    when((df.salary < 4000), lit("A")) \
#      .when((df.salary >= 4000) & (df.salary <= 5000), lit("B")) \
#      .otherwise(lit("C")) \
#   ).show()


# COMMAND ----------

kart_meta = spark.read.json("/FileStore/KartRider/2/3/kart.json")

# COMMAND ----------

kart_meta.registerTempTable("kart_meta")

# COMMAND ----------

df3.columns

# COMMAND ----------

df4 = spark.sql("""
select a.*, b.name
from df_tmp as a
left join kart_meta as b
on 1=1
and a.p_kart = b.id
""")

df4.registerTempTable("df4")

# COMMAND ----------

df3.registerTempTable("df_tmp")

# COMMAND ----------

spark.sql("""
  select channelName, hour(startTime) hour, avg(playTime) avg_playtime
  from df_tmp
  group by channelName, hour(startTime)
""").show()

# COMMAND ----------

ch_hour_avgplaytime = spark.sql("""
  select channelName, hour(startTime) hour, avg(playTime) avg_playtime
  from df_tmp
  group by channelName, hour(startTime)
""").toPandas()

# COMMAND ----------

type(ch_hour_avgplaytime)

# COMMAND ----------

df3.display()

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



# COMMAND ----------

#컬럼 순서 변경
df3 = df3.select('p_accountNo','startTime','matchId','playTime','channelName','trackId','gameSpeed','p_rankinggrade2','teamId','matchResult','teamPlayers','p_matchRank', 'p_matchRetired', 'p_matchWin', 'p_matchTime','p_kart','p_character','p_characterName',   'p_pet', 'p_flyingPet', 'p_partsEngine', 'p_partsHandle', 'p_partsWheel', 'p_partsKit',  'startTime_hour', 'startTime_minute', 'startTime_second', 'startTime_quarter', 'startTime_dayofweek','endTime', 'startTime_month', 'startTime_day','startTime_holiday','name_dayofweek') 

# COMMAND ----------

df3.display()

# COMMAND ----------

# sql 임시 테이블로 저장 (view 저장)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp").display()

# COMMAND ----------

sqlContext.sql("select matchId, teamId, p_matchWin, matchResult from df_tmp").display()

# COMMAND ----------

# 날짜 데이터 정보 확인하던 중, 5월 데이터 확인. 
sqlContext.sql("select *  from df_tmp order by startTime_day desc").display()

# COMMAND ----------

#8 월이 아닌 데이터 확인. => 38개
sqlContext.sql("select * from df_tmp where startTime_month != 8").display()

# COMMAND ----------

#8월이 아닌 데이터 정보 지우기 
df3 = df3.filter(df3.startTime_month == 8)
df3.registerTempTable("df_tmp")
sqlContext.sql("select * from df_tmp where startTime_month != 8").display()

# COMMAND ----------

# 1차 가공한 데이터 저장 
df3.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/week1_matchinfo_df2.csv")
#df3.write.csv("/FileStore/KartRider/week1_matchinfo_df3.csv")

# df3.coalesce(1)
# .write.mode("overwrite")
# .option("header", "true")
# .option("sep", "|")
# .option("encoding", "euc-kr")
# .option("codec", "gzip")
# .csv("/FileStore/KartRider/week1_matchinfo_df3.csv")

# COMMAND ----------

#df4 = df3.drop('_c0')
df4.registerTempTable("df_tmp2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_tmp2

# COMMAND ----------

# MAGIC %sql
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

# MAGIC %sql 
# MAGIC select startTime_hour, count(startTime_hour) as usercnt
# MAGIC from df_tmp2
# MAGIC group by startTime_hour
# MAGIC order by startTime_hour

# COMMAND ----------

pip install python-highcharts

# COMMAND ----------

from highcharts import Highchart

# COMMAND ----------

channel_info = spark.sql("select channelName, count(channelName) as cntchannel, count(p_accountNo) as totaluser, count(distinct p_accountNo) as distinctuser, (sum(p_matchRetired)/count(p_matchRetired)) as retired_ratio  from df_tmp2 group by channelName order by cntchannel")
channel_info.display()

# COMMAND ----------


channel_info["meaning"] = ""
channel_info.at[1, "meaning"] = "초보 채널, 보통"
channel_info.at[3, "meaning"] = "매우 빠름"
channel_info.at[4, "meaning"] = "빠름"
channel_info.at[5, "meaning"] = "매우 빠름"
channel_info.at[6, "meaning"] = "무한 부스터, 빠름"
#초보가 많이 모여, retired_ratio가 높음. 가장 낮은 retired_Ratio 보유한 채널은 매우빠름 방.이용자는 무한 부스터의 경우 가장 많음. 

# COMMAND ----------

# 게임 참여 수와 랭킹 관계 (게임 참여 수와 라이센스 취득간의 관계 유무 확인 )
spark.sql("select p_rankinggrade2, count(p_accountNo) from df_tmp group by p_rankinggrade2 order by p_rankinggrade2").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_hour, count(p_rankinggrade2) as usercnt
# MAGIC from df_tmp2
# MAGIC group by startTime_hour
# MAGIC order by startTime_hour

# COMMAND ----------

type(channel_info)

# COMMAND ----------

channel_info = spark.sql("select channelName, count(channelName) as cntchannel, count(p_accountNo) as totaluser, count(distinct p_accountNo) as distinctuser, (sum(p_matchRetired)/count(p_matchRetired)) as retired_ratio  from df_tmp2 group by channelName")
channel_info.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 다음것 stacked bar charts 로 구현 다시 시도. group by -> df. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select startTime_hour, p_rankinggrade2, count(p_rankinggrade2)
# MAGIC from df_tmp2
# MAGIC group by startTime_hour,p_rankinggrade2 
# MAGIC order by startTime_hour, p_rankinggrade2

# COMMAND ----------

print(count(df3.p_rankinggrade2))

# COMMAND ----------

#for i in range(0, )
df3.groupBy("p_rankinggrade2").count().display()
#stacked bar chart
#df3.groupby('startTime_hour')['p_rankinggrade2'].value_counts().unstack().plot.bar(stacked=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from df_tmp2

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select p_rankinggrade2, count(distinct p_accountNo), count(distinct matchId), (count(matchId)/count(p_accountNo)) 
# MAGIC from df_tmp
# MAGIC group by p_rankinggrade2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_tmp2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_tmp2

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##### 제플린 설치 사이트 
# MAGIC https://zeppelin.apache.org/download.html
# MAGIC ##### 제플린 설치 가이드 
# MAGIC https://blog.naver.com/PostView.nhn?isHttpsRedirect=true&blogId=apple42i&logNo=221373404379

# COMMAND ----------

# MAGIC %sql 
# MAGIC select p_kart, count(p_kart)
# MAGIC from df_tmp2
# MAGIC group by p_kart
# MAGIC order by 2

# COMMAND ----------

# kart id 이름 붙이기 

kartid = spark.read.format('org.apache.spark.sql.json') \
        .load('/FileStore/KartRider/2/3/kart.json')

# COMMAND ----------

sc = spark.sparkContext
path = "/FileStore/KartRider/2/3/kart.json"
kartDF = spark.read.json(path)
kartDF.printSchema()
kartDF.createOrReplaceTempView("karttable")


# COMMAND ----------

kartDF.display()

# COMMAND ----------

kartDF['id'].display()

# COMMAND ----------

kartid.display()

# COMMAND ----------

df.withColumn("p_kart", kartid.name)

# COMMAND ----------

df5 = df3.toPandas()

# COMMAND ----------

df5['p_kart'] = df5['p_kart'].apply(lambda x: kartDF.get(x))

# COMMAND ----------

df5['p_kart'].map(lambda x: kartid.get(x)).

# COMMAND ----------

df5['p_kart'] = df5['p_kart'].apply(lambda x :kartid.get(x))

# COMMAND ----------

result = df5[['playTime','gameSpeed','p_rankinggrade2','matchResult','teamPlayers','p_matchRank','p_matchRetired','p_matchWin','p_matchTime','p_partsEngine','p_partsHandle','p_partsWheel','p_partsKit','startTime_hour','startTime_hour','startTime_holiday']].corr('spearman')
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

answer = df5[['playTime','gameSpeed','p_rankinggrade2','matchResult','teamPlayers','p_matchRank','p_matchRetired','p_matchWin','p_matchTime','p_partsEngine','p_partsHandle','p_partsWheel','p_partsKit','startTime_hour','startTime_hour','startTime_holiday']].corr('pearson')

# COMMAND ----------

import seaborn as sns
ax = sns.heatmap(
    answer, 
    vmin=-1, vmax=1, center=0,
    cmap=sns.diverging_palette(20, 220, n=200),
    square=True
)
ax.set_xticklabels(
    ax.get_xticklabels(),
    rotation=45,
    horizontalalignment='right'
);