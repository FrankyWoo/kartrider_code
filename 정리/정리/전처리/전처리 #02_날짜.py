# Databricks notebook source
#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/all_collected_processed.csv"))


final.registerTempTable("finalsql")

# COMMAND ----------

print((final.count(), len(final.columns)))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from finalsql;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 날짜 전처리 

# COMMAND ----------

spark.sql('select * from finalsql where startTime_month not in (8,9,10) ').display()

# COMMAND ----------

 spark.sql("select * from finalsql where startTime_month = 10").display()

# COMMAND ----------

test = spark.sql('select * from finalsql where startTime_month in (8,9,10)')
print((test.count(), len(test.columns))) ### 데이터 총 개수 : 20515301

# COMMAND ----------

#데이터프레임 csv로도 저장
import pyspark
from pyspark.sql.functions import *

test.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/augtooctdata.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 데이터 분류 (기간 설정을 위해!) 두 가지 방법 </br>
# MAGIC 1) 전체 데이터 기준으로 2:8 되는 기간 확인하여 기간 설정 </br>
# MAGIC 2) 기간을 2:8로 나누기 </br>

# COMMAND ----------

### 1번째. 전체의 0.8 에 해당하는 날짜 확인
import numpy as np 
20515301 * 0.8

# COMMAND ----------

20515301 - 16412240

# COMMAND ----------

### 2번째. 
test.registerTempTable("testsql")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from testsql;

# COMMAND ----------

# MAGIC %md
# MAGIC 결론 : 48일과 13일로 기간을 나누는 것? 

# COMMAND ----------

# MAGIC %md
# MAGIC 고가의 질주 기준으로 하여, 16412240에 해당하는 데이터 찾기. 

# COMMAND ----------

### 고가의 질주 track 정확한 데이터명 확인 
test.groupBy('track').count().show()

# COMMAND ----------

##### '빌리지 고가의 질주 확인' !!!!!!
spark.sql("select * from testsql where track like '%고가%'").groupBy('track').count().show()

# COMMAND ----------

village = spark.sql("select * from testsql where track = '빌리지 고가의 질주' order by startTime")

# COMMAND ----------

print((village.count(), len(village.columns)))

# COMMAND ----------

village.registerTempTable("villagesql")
village.head(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from villagesql limit 10;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(startTime), max(startTime) from villagesql;

# COMMAND ----------

#위의 시간 결과를 통해 village 고가 시작 날짜와 끝나는 날짜 확인 
## 전체 데이터의 80%에 해당하는 데이터 날짜 확인. 
1102326 * 0.8

# COMMAND ----------

1102326 * 0.2

# COMMAND ----------

# MAGIC %md
# MAGIC 데이터가 너무 커서 특정 row 조회가 가능하지 않음을 확인. 
# MAGIC 시간별 매치 count를 통해 확인! 

# COMMAND ----------

spark.sql("select DATE_FORMAT(startTime,'y-M-d'), count(distinct matchid) from villagesql group by DATE_FORMAT(startTime,'y-M-d') order by 1").display()

# COMMAND ----------

spark.sql("select count(distinct matchid) from villagesql where startTime_month = 8 and startTime_day = 1").show()

# COMMAND ----------

spark.sql("select startTime from villagesql  order by 1").display()

# COMMAND ----------

tmp = spark.sql("select DATE_FORMAT(startTime,'y-M-d') as dayday, count(distinct matchid) as cnt from villagesql group by DATE_FORMAT(startTime,'y-M-d') order by 1")

# COMMAND ----------

date = tmp.select('dayday').rdd.flatMap(lambda x: x).collect()
count = tmp.select('cnt').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(list(zip(date, count)), columns =['date', 'count']) 
df

# COMMAND ----------

import plotly.express as px

fig = px.bar(df, x='date', y='count')
fig.show()