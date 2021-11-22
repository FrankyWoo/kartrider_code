# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
# 가공한 데이터 파일 오픈. 
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/week1_matchinfo_df2.csv"))
#df.display() 


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #분석 (2021/09/14)
# MAGIC 1. 날짜별 : 1주일 간 이용자 빈도 수 확인
# MAGIC 2. 채널별 : 채널별 1주일간 이용자 수, 리타이어율, 요일별 채널 수 비교. 

# COMMAND ----------

df.registerTempTable("df_tmp")

# COMMAND ----------

spark.sql("select * from df_tmp").display()

# COMMAND ----------

channelinfo = spark.sql("select channelName, count(channelName) as cnt, (sum(p_matchRetired)/count(p_matchRetired)) as retired_ratio from df_tmp group by channelName")
channelinfo.display()


# COMMAND ----------

channelinfo = channelinfo.select("*").toPandas()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

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

channel2 = spark.sql("select channelName, count(channelName) as cnt, name_dayofweek from df_tmp group by channelName, name_dayofweek order by channelName")
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

#라이센스와 채널의 상관관계 
df["p_rankinggrade2", "channelName"].display()

# COMMAND ----------

df = df.select("*").toPandas()

# COMMAND ----------

import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns
plt.figure(figsize=(15, 6))
#sns.barplot(x="cnt", hue="channelName", y="retired_ratio", data=channelinfo)
sns.barplot(x="channelName", y="retired_ratio", data=channelinfo)
plt.show()

# COMMAND ----------

# libraries
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
 
# Data
#df=pd.DataFrame({'x_values': range(1,11), 'y1_values': np.random.randn(10), 'y2_values': np.random.randn(10)+range(1,11), 'y3_values': np.random.randn(10)+range(11,21) })
 
# multiple line plots
plt.plot( 'name_dayofweek', 'y1_values', data=channel2, marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4)
plt.plot( 'name_dayofweek', 'y2_values', data=channel2, marker='', color='olive', linewidth=2)
plt.plot( 'name_d', 'y3_values', data=channel2, marker='', color='olive', linewidth=2, linestyle='dashed', label="toto")

# show legend
plt.legend()

# show graph
plt.show()

# COMMAND ----------

columns_to_drop = ['Unnamed: 0', 'p_license', '_c0']
df3 = df3.drop(*columns_to_drop)
df3.display()

# COMMAND ----------

1. 라이센스별 평균 게임 이용시간. 라이센스별 
2. channel 세분화 정보 

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

df3.registerTempTable("df_tmp")

# COMMAND ----------

spark.sql("""
  select * from df_tmp
""").display()

# COMMAND ----------

channel_info = spark.sql("select channelName, count(channelName) as cntchannel, count(p_accountNo) as totaluser, count(distinct p_accountNo) as distinctuser, (sum(p_matchRetired)/count(p_matchRetired)) as retired_ratio  from df_tmp group by channelName")
channel_info.display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, max(p_rankinggrade2)")

# COMMAND ----------

a = channel_info['channelName'].to_list()
b = channel_info['cntchannel'].to_list()

# COMMAND ----------

rrr = channel_info[['channelName', 'count(channelNAme)']].display()

# COMMAND ----------

import matplotlib.pyplot as plt
f, ax = plt.subplots(1,1,figsize=(16,8))
plt.plot(exone.sdate, exone.playtime, color='green', label='target')
plt.plot(extwo.sdate, extwo.playtime, color='blue', label='control')
plt.plot(exone.sdate, exone.playtime, color='green', label='target')
plt.plot(extwo.sdate, extwo.playtime, color='blue', label='control')
plt.plot(exone.sdate, exone.playtime, color='green', label='target')
plt.plot(extwo.sdate, extwo.playtime, color='blue', label='control')

plt.xlabel("date")
plt.ylabel("playtime")
plt.title("date and playtime")
plt.axvline(x='2020-11-19',  linestyle='--', color='red', label="message")
plt.legend()
plt.show()

# COMMAND ----------

# 전체에 대해 세그먼트 분리 시키기. 
spark.sql("select * from df_tmp").display()

# COMMAND ----------

# 게임 참여 수와 랭킹 관계 (게임 참여 수와 라이센스 취득간의 관계 유무 확인 )
spark.sql("select p_rankinggrade2, count(p_accountNo) from df_tmp group by p_rankinggrade2 order by p_rankinggrade2").display()