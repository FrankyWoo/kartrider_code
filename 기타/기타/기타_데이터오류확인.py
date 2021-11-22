# Databricks notebook source
pip install distfit

# COMMAND ----------

import distfit
print(distfit.__version__)

# COMMAND ----------

from distfit import distfit
import pandas as pd
import numpy as np

# COMMAND ----------

#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/lastone4.csv"))

final.registerTempTable("finalsql")
#df = final.select("*").toPandas()

# COMMAND ----------

final.display()

# COMMAND ----------

#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/total.csv"))

final.registerTempTable("finalsql")
#df = final.select("*").toPandas()

# COMMAND ----------

final.display()

# COMMAND ----------

tmp_list = final.select('playTime').collect()

# COMMAND ----------

tmp_list

# COMMAND ----------

# playTime -> list 
X = tmp_list
X = np.array(X, dtype=np.float32)
dist = distfit()
dist.fit_transform(X)
dist.plot()
# All scores of the tested distributions
print(dist.summary)
# Distribution parameters for best fit
dist.model

# Make plot
dist.plot_summary()

# COMMAND ----------

final.groupBy('playTime').count().orderBy('playTime').display()

# COMMAND ----------

tmp2 = final.groupBy('playTime').count().orderBy('playTime')
tmp2.display()

# COMMAND ----------

tmp2.display()

# COMMAND ----------

tmp2.describe(['playTime']).show()

# COMMAND ----------

spark.sql("select * from finalsql where playTime == '93807'").display()

# COMMAND ----------

#같은 놈일 것 같다..?
spark.sql("select * from finalsql where p_accountNo == '1493715193'").display()

# COMMAND ----------

spark.sql("select * from finalsql where p_accountNo == '50929301'").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC 추가 가공 필요! 

# COMMAND ----------

spark.sql("select count(*) from finalsql where teamPlayers = '[22222222222222]' ").display()

# COMMAND ----------

spark.sql("select teamPlayers, teamPlayers2 from finalsql2 where teamPlayers = '[22222222222222]' limit 1 ").display()

# COMMAND ----------

spark.sql("select * from finalsql2 where teamPlayers2 = '[22222222222222]' limit 1").display()

# COMMAND ----------

final.registerTempTable("finalsql")

# COMMAND ----------

final.display()

# COMMAND ----------

len('22222222222222')

# COMMAND ----------

len('22222222222222')

# COMMAND ----------

final2 = final.withColumn("teamPlayers2", regexp_replace("teamPlayers", "\[22222222222222\]", "2"))
final2.display()


# COMMAND ----------

final2.registerTempTable("finalsql2")

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace
final = final.withColumn('teamPlayers2', 
    when(final.teamPlayers.endswith('[22222222222222]'),regexp_replace(final.teamPlayers,"[22222222222222]",'2')) 
   .otherwise(final.teamPlayers))

# COMMAND ----------

import pandas as pd
total_df = pd.read_csv("/dbfs/FileStore/KartRider/total.csv/part-00000-tid-1656583723389088856-eb8eb97c-ed4e-4196-9d55-b40d731bb8eb-1056-1-c000.csv", header='infer') 

# COMMAND ----------

matchinfo_total_df = pd.read_csv("/dbfs/FileStore/KartRider/2/3/matchinfo_total_df.csv", header='infer') 

# COMMAND ----------

# MAGIC %md
# MAGIC actual distance 구하기 

# COMMAND ----------

# MAGIC %md 
# MAGIC boxplot 활용하여 변수별 분포를 playTime을 기준으로 확인 => distfit 이용! 

# COMMAND ----------

# MAGIC %md 
# MAGIC barplot 활용하여 변수별 분포 확인. disfit 말고 일단 seaborn 만 이용. playTime 기준. 

# COMMAND ----------

# MAGIC %md track별 분포 다시 확인

# COMMAND ----------

trackbasic = spark.sql("select track, playTime from finalsql")

# COMMAND ----------

trackbasic.display()

# COMMAND ----------

spark.sql("select track, count(*) from finalsql group by track").display() #총 283

# COMMAND ----------

### 빌리지 고가의 지주 하나 픽! 
village = spark.sql("select * from finalsql2 where track = '빌리지 고가의 질주'")

# COMMAND ----------

print((village.count(), len(village.columns)))

# COMMAND ----------

test1 = spark.sql("select track, channelName, teamPlayers2, p_rankinggrade2, (sum(p_matchWin)/count(*)) as winrate, (sum(p_matchRetired)/count(*)) as retiredrate, collect_list(playTime) as playtime from finalsql2 group by track, channelName, teamPlayers2, p_rankinggrade2 order by track, channelName, teamPlayers2, p_rankinggrade2")

# COMMAND ----------

test1.display()

# COMMAND ----------

spark.sql("select  track,teamPlay,ers, channelName,p_rankinggrade2, collect_list(playTime) from finalsql group by track, teamPlayers,channelName,p_rankinggrade2 order by teamPlayers, track, p_rankinggrade2 limit 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 기본 의심 정보 위주로 파악. 

# COMMAND ----------

# MAGIC %md
# MAGIC teamplayers = 1

# COMMAND ----------

one = spark.sql("select * from finalsql2 where teamPlayers = 1")
print((one.count(), len(one.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC teamplayers = 2

# COMMAND ----------

# MAGIC %md
# MAGIC teamplayers = 3

# COMMAND ----------

three = spark.sql("select * from finalsql where teamPlayers = 3")
print((three.count(), len(three.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC teamplayers = 4

# COMMAND ----------

four = spark.sql("select * from finalsql where teamPlayers = 4")
print((four.count(), len(four.columns)))