# Databricks notebook source
df = (spark.read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))


df.registerTempTable("df")

track_temp=df.toPandas()

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

total = august.union(september)
total.registerTempTable("total")

# COMMAND ----------

#난이도별 평균 매치타임

%sql

select difficulty, avg(p_matchTime)
from total
group by difficulty


# COMMAND ----------

#시각화를 위한 제한적 데이터 추출

temp=spark.sql("select p_matchTime from df order by rand() limit 100000")

# COMMAND ----------

#매치타임 히스토그램

import plotly.express as px
temp_pd=temp.toPandas()

fig=px.histogram(temp_pd,x="p_matchTime")
fig.show()

# COMMAND ----------

#매치타임 박스플롯

fig=px.box(temp_pd,x="p_matchTime",points="all")
fig.update_traces(quartilemethod="inclusive")
fig.show()

# COMMAND ----------

#IsolationForest 결과 데이터프레임 업로드

# /FileStore/KartRider/if_param_test/max_nestimators_c0.01_non_track.csv

# result = (spark.read                                              
#   .option("inferSchema","true")                 
#   .option("header","true")                           
#   .csv("/FileStore/KartRider/if_param_test/edited_result_non_track.csv"))

result1 = (spark.read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/if_param_test/edited_result_non_track.csv"))
# properc_edited_result_non_track.csv
result1.registerTempTable("result_meta_table")

selected_temp_a=spark.sql("select * from result_meta_table order by rand() limit 100000")

result_meta=selected_temp_a.toPandas()
result_meta=result1.toPandas()


result2 = (spark.read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/if_param_test/result_combinated_50_200_0.25.csv"))
# properc_edited_result_non_track.csv
result2.registerTempTable("result_combinated_table")

selected_temp_b=spark.sql("select * from result_combinated_table order by rand() limit 100000")

result_combinated=selected_temp_b.toPandas()

result3 = (spark.read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/if_param_test/result_track_only_50_150_0.5auto.csv"))
# properc_edited_result_non_track.csv
result3.registerTempTable("result_track_table")

selected_temp_c=spark.sql("select * from result_track_table order by rand() limit 100000")

result_track=selected_temp_c.toPandas()

# COMMAND ----------

result_meta

# COMMAND ----------

#데이터프레임에서 시각화에 필요한 정보만 추출

meta_result_temp=result_meta[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
combinated_result_temp=result_combinated=result_combinated[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
track_result_temp=result_track[["p_matchTime","score0.075","anomal_clustered_label0.075"]]

# COMMAND ----------

import plotly.express as px
fig = px.scatter(meta_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

import plotly.express as px
fig = px.scatter(combinated_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

import plotly.express as px
fig = px.scatter(track_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

# temp = (spark.read                                              
#   .option("inferSchema","true")                 
#   .option("header","true")                           
#   .csv("/FileStore/KartRider/if_param_test/edited_result_non_track.csv"))

temp = (spark.read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/if_param_test/versionA_track_result_rr_added.csv"))
temp.registerTempTable("temp")

# COMMAND ----------

# 트랙9 0.6171296249379288
# 붐힐 드라이브 0.23277110399543147
# 빌리지 운하 0.07988944411183171


# COMMAND ----------

temp.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from temp 
# MAGIC -- where retiredRatio=0.23277110399543147

# COMMAND ----------

#트랙9,빌리지운하,붐힐드라이브,광산꼬불꼬불,빌리지 고가에 대해 시각화
#retiredRatio가 유니크하므로 데이터 추출의 기준이 된다

t9=spark.sql("select * from temp where retiredRatio=0.6171296249379288")
boomhill=spark.sql("select * from temp where retiredRatio=0.23277110399543147")
woonha=spark.sql("select * from temp where retiredRatio=0.07988944411183171")
gwangsan=spark.sql("select * from temp where retiredRatio=0.38514227295961834")
goga=spark.sql("select * from temp where retiredRatio=0.0895162332533995810")

track9=t9.toPandas()
villeage_boomhill=boomhill.toPandas()
villeage_woonha=woonha.toPandas()
gwangsan_kkobul=gwangsan.toPandas()
villeage_goga=goga.toPandas()

# COMMAND ----------

boomhill.display()

# COMMAND ----------

#트랙
track9_result_temp=track9[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
vbh_result_temp=villeage_boomhill[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
vw_result_temp=villeage_woonha[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
gk_result_temp=gwangsan_kkobul[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
vg_result_temp=villeage_goga[["p_matchTime","score0.075","anomal_clustered_label0.075"]]

# track9_result_temp=track9[["p_matchTime","score0.1","anomal_clustered_label0.1"]]
# vbh_result_temp=villeage_boomhill[["p_matchTime","score0.1","anomal_clustered_label0.1"]]
# vw_result_temp=villeage_woonha[["p_matchTime","score0.1","anomal_clustered_label0.1"]]

# COMMAND ----------

#트랙9
import plotly.express as px
fig = px.scatter(track9_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

#붐힐드라이브
import plotly.express as px
fig = px.scatter(vbh_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

#빌리지 운하
import plotly.express as px
fig = px.scatter(vw_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

#광산꼬불
import plotly.express as px
fig = px.scatter(gk_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------

#빌리지 고가
import plotly.express as px
fig = px.scatter(vg_result_temp, x="p_matchTime", y="score0.075",color="anomal_clustered_label0.075")
fig.show()

# COMMAND ----------



# COMMAND ----------

#전체 데이터에 대해 IsolationForest 분석한 결과
#기존 데이터에서 트랙만 갖고오고 기존 데이터를 분석한 결과 데이터프레임의 매치타임,스코어,라벨을 결합

import pandas as pd

track_df=track_temp[["track"]]
meta_result_temp=result_meta[["p_matchTime","score0.075","anomal_clustered_label0.075"]]
concated=pd.concat([track_df,meta_result_temp],axis=1)
concated

# COMMAND ----------

concated.rename(columns = {'score0.075' : 'score'}, inplace = True)
concated.rename(columns = {'anomal_clustered_label0.075' : 'label'}, inplace = True)


