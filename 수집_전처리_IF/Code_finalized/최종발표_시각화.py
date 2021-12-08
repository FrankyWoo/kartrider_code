# Databricks notebook source
#데이터로드


# df = (spark.read                                              
#   .option("inferSchema","true")                 
#   .option("header","true")                           
#   .csv("/FileStore/KartRider/if_param_test/edited_result_non_track.csv"))
# df.registerTempTable("df")

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/august4.csv"))

august.registerTempTable("august")

september = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/september4.csv"))

september.registerTempTable("september")

total = august.union(september)
total.registerTempTable("total")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select * from total where lap2=3

# COMMAND ----------

#리타이어비율 제대로 통합하기 위한 수정코드

%sql

with a as(
select track,sum(p_matchRetired)/count(p_matchRetired) as retiredRatio
from total
group by track)
select * 
from total left join a 
on total.track=a.track

# COMMAND ----------

sample_spark=spark.sql("with a as(select track as track_temp,sum(p_matchRetired)/count(p_matchRetired) as retiredRatio from total group by track) select * from total left join a on total.track=a.track_temp")
sample_spark.registerTempTable("rr_added")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rr_added

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select track, difficulty, avg(p_matchTime) as avg_mt, count(difficulty) as cnt , retiredRatio,  rank() over (order by avg(p_matchTime) asc) mt_rank, rank() over (order by count(difficulty) desc) cnt_rank,rank() over (order by count(difficulty) desc) retired_rank from rr_added where track in ('트랙 9','빌리지 운하','붐힐 드라이브','빌리지 고가의 질주','광산 꼬불꼬불 다운힐') group by difficulty,track,retiredRatio

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC # 시간별 매치타임 및 정보
# MAGIC 
# MAGIC select startTime_hour,channelName,track,p_rankinggrade2,teamPlayers,count(matchId) as total_cnt,count(distinct matchId) as match_cnt, count(distinct p_accountNo) as user_cnt,sum(p_matchRetired) as retired_cnt,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from rr_added group by startTime_hour,channelName,track,p_rankinggrade2,teamPlayers order by startTime_hour,channelName,track,p_rankinggrade2,teamPlayers 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC #월/날짜별 매치타임 및 정보
# MAGIC 
# MAGIC select startTime_month,startTime_day,channelName,track,p_rankinggrade2,teamPlayers,count(matchId) as total_cnt,count(distinct matchId) as match_cnt, count(distinct p_accountNo) as user_cnt,sum(p_matchRetired) as retired_cnt,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from rr_added group by startTime_month,startTime_day,channelName,track,p_rankinggrade2,teamPlayers order by startTime_month,startTime_day,channelName,track,p_rankinggrade2,teamPlayers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC #트랙별 매치타임 및 정보
# MAGIC 
# MAGIC select track,channelName,p_rankinggrade2,teamPlayers,count(matchId) as total_cnt,count(distinct matchId) as match_cnt, count(distinct p_accountNo) as user_cnt,sum(p_matchRetired) as retired_cnt,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from rr_added group by track,channelName,p_rankinggrade2,teamPlayers order by track, channelName,p_rankinggrade2,teamPlayers 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC #난이도별 매치타임 및 정보
# MAGIC 
# MAGIC select difficulty,track,channelName,p_rankinggrade2,teamPlayers,count(matchId) as total_cnt,count(distinct matchId) as match_cnt, count(distinct p_accountNo) as user_cnt,sum(p_matchRetired) as retired_cnt,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from total group by difficulty,track,channelName,p_rankinggrade2,teamPlayers order by difficulty,track, channelName,p_rankinggrade2,teamPlayers 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC #랩별 매치타임 및 정보
# MAGIC 
# MAGIC select lap2,track,channelName,p_rankinggrade2,teamPlayers,count(matchId) as total_cnt,count(distinct matchId) as match_cnt, count(distinct p_accountNo) as user_cnt,sum(p_matchRetired) as retired_cnt,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from total group by lap2,difficulty,track,channelName,p_rankinggrade2,teamPlayers order by lap2,difficulty,track, channelName,p_rankinggrade2,teamPlayers 

# COMMAND ----------

#위의 테이블을 파켓으로 저장해놓은 부분

#sample_spark=spark.sql("select actualdistance,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from total group by actualdistance")
#sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/distance_mt_scatter_2.parquet")
#sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/grouped_by_date_2.parquet")
#sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/grouped_by_hour.parquet")
# sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/grouped_by_date.parquet")
# sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/grouped_by_difficulty.parquet")
# sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/grouped_by_lap.parquet")
# sample_spark.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/distance_avg(mt)_scatter.parquet")

# COMMAND ----------



# COMMAND ----------

#난이도별 평균 매치타임

%sql

select difficulty, avg(p_matchTime) as avg_mt
from total
group by difficulty


# COMMAND ----------

#라이센스별 평균 난이도, 평균 매치타임

%sql

select p_rankinggrade2, avg(difficulty) as avg_difficulty,avg(p_matchTime) as avg_mt
from total
group by p_rankinggrade2

# COMMAND ----------

#팀원수별 평균 매치타임

%sql 
select teamPlayers,avg(p_matchTime) as avg_mt 
from total 
group by teamPlayers


# COMMAND ----------

#실주행거리별 매치타임 통계량

%sql

select actualdistance,avg(p_matchTime) as avg_mt,stddev(p_matchTime) as std_mt ,approx_percentile(p_matchTime,0.5) med_mt,approx_percentile(p_matchTime,0.25) as q1_mt,approx_percentile(p_matchTime,0.75) as q3_mt from total group by actualdistance 

# COMMAND ----------

#리아티어비율과 평균매치타임의 산점도

%sql

select retiredRatio,avg(p_matchTime) as avg_mt from df group by retiredRatio

# COMMAND ----------

#난이도별 평균 리타이어 비율

%sql

select difficulty,avg(retiredRatio) as avg_retiredratio from df group by difficulty

# COMMAND ----------

#트랙별 평균 매치타임

%sql

select track,trackId,avg(p_matchTime)as avg_mt  from total group by track,trackId

# COMMAND ----------

#매치타임이 40보다 작았던것

%sql 

select p_matchTime from total where p_matchTime<40

# COMMAND ----------

sample_spark=spark.sql("select p_matchTime from total where p_matchTime<40 ")
sample=sample_spark.toPandas()

# COMMAND ----------

#40보다 작았던 매치타임 히스토그램

import plotly.express as px
fig = px.histogram(sample, x="p_matchTime")
fig.show()

# COMMAND ----------

sample_spark=spark.sql("select p_matchTime from total where p_matchTime>600 ")
sample=sample_spark.toPandas()

# COMMAND ----------

import plotly.express as px
fig = px.histogram(sample, x="p_matchTime")
fig.show()

# COMMAND ----------

august = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/ec2_eda/grouped_by_lap.parquet"))