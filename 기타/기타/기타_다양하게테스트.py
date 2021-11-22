# Databricks notebook source
#파일 open
#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv"))

final.registerTempTable("final_df")


# COMMAND ----------

final3df.to_csv('/dbfs/FileStore/KartRider/final3df.csv')

# COMMAND ----------

spark.sql("select * from final_df").display()

# COMMAND ----------

abnewbie = spark.sql("select * from final_df where (channelName like '%Newbie') and (p_rankinggrade2 >3) ")  #총 521개
abnewbie.display()

# COMMAND ----------

finalsql = spark.sql("select * from final_df where not channelName like '%Newbie' ")
finalsql.display()

# COMMAND ----------

finalsql.registerTempTable("finalsql2")
spark.sql("select channelName, count(*) from finalsql2 group by channelName").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ............다시... ----

# COMMAND ----------

finalsql.registerTempTable("finalsql2")
spark.sql("select channelName, count(*) from finalsql2 group by channelName").display()

# COMMAND ----------

abnewbie.registerTempTable("abnewbiesql")
spark.sql("select channelName, count(*) from abnewbiesql group by channelName").display()

# COMMAND ----------

normalnewbie = spark.sql("select * from final_df where (channelName like '%Newbie') and (p_rankinggrade2 < 4)")
normalnewbie.display()

# COMMAND ----------

normalnewbie.registerTempTable("normalsql")
spark.sql("select channelName, count(*) from normalsql group by channelName").display()

# COMMAND ----------

spark.sql("select p_rankinggrade2, count(*) from normalsql group by p_rankinggrade2").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC 최종 테이블 : finalsql , sql: finalsql2

# COMMAND ----------

1197-120

# COMMAND ----------

## 인원 확인
final2 = finalsql.select("*").toPandas()
normal = normalnewbie.select("*").toPandas()
final2['teamPlayers'].value_counts() #총 2명, 4명, 6명, 8명

# COMMAND ----------

final3 = spark.sql("(select * from finalsql2) union (select * from normalsql)")
final3df = final3.select("*").toPandas()
final3.registerTempTable("final3sql")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ###최종 테이블 !!!! : final3

# COMMAND ----------

#파일 open
#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/normaldata1.csv"))

final.registerTempTable("final_df")


# COMMAND ----------

final3df = final.select("*").toPandas()

# COMMAND ----------

final3df.groupby("teamPlayers")[['playTime','p_rankinggrade2','channelName']].describe() #channelName #PlayTime구간에 따라. 

# COMMAND ----------

final3df.groupby("teamPlayers")[['trackname','p_matchRank', 'p_matchRetired', 'p_matchWin','kartname' ]].describe() #trackname, kartname

# COMMAND ----------

type(final2['distance'][179])#[:-2]

# COMMAND ----------

def get_distance(x):
  if pd.isna(x) != True:
    return x[:-2]
  else:
    return x

# COMMAND ----------

final3df['distance'] = final3df['distance'].astype("string")

# COMMAND ----------

final2[final2['distance'].isnull() == True]

# COMMAND ----------

import pandas as pd
final3df['distance'] = final3df['distance'].apply(lambda x : get_distance(x))
final3df.head()

# COMMAND ----------

#final2['distance'] = pd.to_numeric(final2['distance'], errors='coerce')
final3df['distance'] = pd.to_numeric(final3df['distance'])

# COMMAND ----------

import seaborn as sns
tmp_playTime = final3df[['teamPlayers', 'playTime']]
tmp_rankinggrade2 = final3df[['teamPlayers', 'p_rankinggrade2']]
tmp_matchRank = final3df[['teamPlayers', 'p_matchRank']]
tmp_matchRetired = final3df[['teamPlayers', 'p_matchRetired']]
tmp_matchWin = final3df[['teamPlayers', 'p_matchWin']]
tmp_distance = final3df[['teamPlayers', 'distance']]
#sns.lineplot(data = tmp, x = "teamPlayers", y="playTime", markers = True, dashes = False)

# COMMAND ----------

### boxplot을 어떻게 나눌지 고민. 
import seaborn as sns
from matplotlib import pyplot as plt
sns.set() # 
fig, axes = plt.subplots(2, 3, figsize=(18, 10))

fig.suptitle('Stats by Number of players')

sns.boxplot(ax=axes[0, 0], data=tmp_playTime, x='teamPlayers', y='playTime')
sns.boxplot(ax=axes[0, 1], data=tmp_rankinggrade2, x='teamPlayers', y='p_rankinggrade2')
sns.boxplot(ax=axes[0, 2], data=tmp_matchRank, x='teamPlayers', y='p_matchRank')
sns.boxplot(ax=axes[1, 0], data=tmp_matchRetired, x='teamPlayers', y='p_matchRetired')
sns.boxplot(ax=axes[1, 1], data=tmp_matchWin, x='teamPlayers', y='p_matchWin')
sns.boxplot(ax=axes[1, 2], data=tmp_distance, x='teamPlayers', y='distance')   

# COMMAND ----------

final3df[['teamPlayers', 'playTime', 'p_rankinggrade2', 'p_matchRank', 'p_matchRetired', 'p_matchWin', 'distance']].corr()

# COMMAND ----------

# MAGIC %md
# MAGIC * target value => playTime
# MAGIC * playTime을 예측하는 모델을 통한 이상치 탐지. 

# COMMAND ----------

# MAGIC %md
# MAGIC 파란색 부스터를 팀 부스터라고 하는데 스피드 팀전에서 부스터를 쓰면서 부스터가 생기면 밑에서 파란색 게이지가 많아집니다. 

# COMMAND ----------

test = spark.sql("select channelName,  teamPlayers, playTime from final3sql order by channelName, teamPlayers")
test.display()

# COMMAND ----------

final3df.to_csv('')

# COMMAND ----------

test.registerTempTable("testsql")
spark.sql("select channelName, count(channelName) from testsql group by channelName").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------------------------speedTeamInfinit-----------------------------------------------------------------------------------------------------

# COMMAND ----------

#teammembers :1,2,3,4 
## channelName : speedTeamInfinit
test1 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamInfinit'")
test1.display()

# COMMAND ----------

test1df = test1.select("*").toPandas()
sns.scatterplot(data=test1df, x="teamPlayers", y="playTime")

# COMMAND ----------

test11 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 1")
test11.display()

# COMMAND ----------

test12 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 2")
test12.display()

# COMMAND ----------

test13 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 3")
test13.display()

# COMMAND ----------

test14 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 4")
test14.display()

# COMMAND ----------

test11df = test11.select("*").toPandas()
test12df = test12.select("*").toPandas()
test13df = test13.select("*").toPandas()
test14df = test14.select("*").toPandas()

# COMMAND ----------

print("--------------test11--------------")
print(test11df['playTime'].quantile(0.99))
print(test11df['playTime'].quantile(0.01))
print("--------------test12--------------")
print(test12df['playTime'].quantile(0.99))
print(test12df['playTime'].quantile(0.01))
print("--------------test13--------------")
print(test13df['playTime'].quantile(0.99))
print(test13df['playTime'].quantile(0.01))
print("--------------test14--------------")
print(test14df['playTime'].quantile(0.99))
print(test14df['playTime'].quantile(0.01))

# COMMAND ----------

test11.registerTempTable("test11sql")
test12.registerTempTable("test12sql")
test13.registerTempTable("test13sql")
test14.registerTempTable("test14sql")

# COMMAND ----------

print(spark.sql("select count(*) from test11sql where playTime < 72 or playTime > 306").display())
spark.sql("select * from test11sql where playTime < 72 or playTime > 306").display() #13094/711924

# COMMAND ----------

print(spark.sql("select count(*) from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 1").display())
print(spark.sql("select count(*) from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 2").display())
print(spark.sql("select count(*) from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 3").display())
print(spark.sql("select count(*) from testsql where channelName == 'speedTeamInfinit' and teamPlayers == 4").display())

# COMMAND ----------

print(spark.sql("select count(*) from test12sql where playTime < 73 or playTime > 193").display())
spark.sql("select * from test12sql where playTime < 73 or playTime > 193").display() #6673/336342

# COMMAND ----------

print(spark.sql("select count(*) from test13sql where playTime < 73 or playTime > 174").display())
spark.sql("select * from test13sql where playTime < 73 or playTime > 174").display #2317/140050

# COMMAND ----------

print(spark.sql("select count(*) from test14sql where playTime < 73 or playTime > 156").display())
spark.sql("select * from test14sql where playTime < 73 or playTime > 156").display() #4871/
1352829

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------speedTeamFastNewbie-----------------------------------------------------------------------------

# COMMAND ----------

testa = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamFastNewbie'")
testa.display()

# COMMAND ----------

print(spark.sql("select count(*) from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 1").display())
print(spark.sql("select count(*) from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 2").display())
print(spark.sql("select count(*) from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 3").display())
print(spark.sql("select count(*) from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 4").display())

# COMMAND ----------

testa1 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 1")
testa2 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 2")
testa3 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 3")
testa4 = spark.sql("select teamPlayers, playTime from testsql where channelName == 'speedTeamFastNewbie' and teamPlayers == 4")

# COMMAND ----------

testa1df = testa1.select("*").toPandas()
testa2df = testa2.select("*").toPandas()
testa3df = testa3.select("*").toPandas()
testa4df = testa4.select("*").toPandas()

# COMMAND ----------

print("--------------test11--------------")
print(testa1df['playTime'].quantile(0.99))
print(testa1df['playTime'].quantile(0.01))
print("--------------test12--------------")
print(testa2df['playTime'].quantile(0.99))
print(testa2df['playTime'].quantile(0.01))
print("--------------test13--------------")
print(testa3df['playTime'].quantile(0.99))
print(testa3df['playTime'].quantile(0.01))
print("--------------test14--------------")
print(testa4df['playTime'].quantile(0.99))
print(testa4df['playTime'].quantile(0.01))

# COMMAND ----------

testa1.registerTempTable("testa1sql")
testa2.registerTempTable("testa2sql")
testa3.registerTempTable("testa3sql")
testa4.registerTempTable("testa4sql")

# COMMAND ----------

print(spark.sql("select count(*) from testa1sql where playTime < 77 or playTime > 285").display())
print(spark.sql("select count(*) from testa2sql where playTime < 77 or playTime > 189").display())
print(spark.sql("select count(*) from testa3sql where playTime < 77 or playTime > 178").display())
print(spark.sql("select count(*) from testa4sql where playTime < 77 or playTime > 172").display())
print(spark.sql("select * from testa1sql where playTime < 77 or playTime > 285").display()) #36297
print(spark.sql("select * from testa2sql where playTime < 77 or playTime > 189").display()) #43775
print(spark.sql("select * from testa3sql where playTime < 77 or playTime > 178").display()) #35555
print(spark.sql("select * from testa4sql where playTime < 77 or playTime > 172").display()) #109198

# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------track 추가 : 빌리지 고가의 질주 -------------------------------------------------------

# COMMAND ----------

village = spark.sql("select * from final3sql where track == '빌리지 고가의 질주'")
village.display()

# COMMAND ----------

village.registerTempTable("villagesql")
print(spark.sql("select count(*) from villagesql where channelName == 'speedTeamInfinit' ").display())
spark.sql("select * from villagesql where channelName == 'speedTeamInfinit' ").display() 

# COMMAND ----------

village2 = village.select("*").toPandas()
village2['channelName'].value_counts()

# COMMAND ----------

village1 = spark.sql("select teamPlayers, playTime from villagesql where channelName == 'speedTeamInfinit' and teamPlayers == 1")
village2 = spark.sql("select teamPlayers, playTime from villagesql where channelName == 'speedTeamInfinit' and teamPlayers == 2")
village3 = spark.sql("select teamPlayers, playTime from villagesql where channelName == 'speedTeamInfinit' and teamPlayers == 3")
village4 = spark.sql("select teamPlayers, playTime from villagesql where channelName == 'speedTeamInfinit' and teamPlayers == 4")

# COMMAND ----------

village1df = village1.select("*").toPandas()
village2df = village2.select("*").toPandas()
village3df = village3.select("*").toPandas()
village4df = village4.select("*").toPandas()

# COMMAND ----------

print("--------------test11--------------")
print(village1df['playTime'].quantile(0.99))
print(village1df['playTime'].quantile(0.01))
print("--------------test12--------------")
print(village2df['playTime'].quantile(0.99))
print(village2df['playTime'].quantile(0.01))
print("--------------test13--------------")
print(village3df['playTime'].quantile(0.99))
print(village3df['playTime'].quantile(0.01))
print("--------------test14--------------")
print(village4df['playTime'].quantile(0.99))
print(village4df['playTime'].quantile(0.01))

# COMMAND ----------

village1.registerTempTable("village1sql")
village2.registerTempTable("village2sql")
village3.registerTempTable("village3sql")
village4.registerTempTable("village4sql")

# COMMAND ----------

print(spark.sql("select count(*) from village1sql where playTime < 110 or playTime > 222").display())
print(spark.sql("select count(*) from village2sql where playTime < 110 or playTime > 145").display())
print(spark.sql("select count(*) from village3sql where playTime < 109 or playTime > 138").display())
print(spark.sql("select count(*) from village4sql where playTime < 109 or playTime > 120").display())
print(spark.sql("select * from village1sql where playTime < 110 or playTime > 222").display()) 
print(spark.sql("select * from village2sql where playTime < 110 or playTime > 145").display()) 
print(spark.sql("select * from village3sql where playTime < 109 or playTime > 138").display()) 
print(spark.sql("select * from village4sql where playTime < 109 or playTime > 120").display()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 정상범위로 -====> 회귀모델 

# COMMAND ----------

import pandas as pd
from scipy import stats
from statsmodels.stats import weightstats as stests
ztest ,pval = stests.ztest(test1df['playTime'], x2=None, value=156)
print(float(pval))
if pval<0.05:
    print("reject null hypothesis")
else:
    print("accept null hypothesis")