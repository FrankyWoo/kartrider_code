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

spark.sql("select * from final_df where playTime < 11").display()
# 총 90개. "="

# COMMAND ----------

spark.sql("select p_accountNo, count(p_accountNo), sum(p_matchRetired), sum(p_matchWin) from final_df where playTime < 11 group by p_accountNo order by count(p_accountNo)").display()

# COMMAND ----------

spark.sql("select matchId, count(matchId) from final_df where playTime < 11 group by matchId order by count(matchId)").display()

# COMMAND ----------

spark.sql('select p_accountNo, matchId, playTime from final_df where p_accountNo== "1174988487" ').display()

# COMMAND ----------

spark.sql('select p_accountNo, matchId, playTime, p_matchRetired, p_matchWin, p_matchRank from final_df where matchId== "01f20021b6e0dc64" ').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 결론 : 1174988487 유저 문제 있음. => 컴퓨터 재부팅 유저 정보로 조회. 

# COMMAND ----------

final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")
 # .csv("/FileStore/KartRider/normaldata1.csv"))
  .csv("/FileStore/KartRider/final3df.csv"))

final.registerTempTable("final_df")

# COMMAND ----------

final2 = final.select("*").toPandas()

# COMMAND ----------

normal = spark.sql("select * from final_df where playTime > 11")
normal.display()

# COMMAND ----------

del normal['_c0']

# COMMAND ----------

normal = normal.select("*").toPandas()

# COMMAND ----------

normal.head()

# COMMAND ----------

normal.head()

# COMMAND ----------

normal.to_csv('/dbfs/FileStore/KartRider/normaldata1.csv')

# COMMAND ----------

normaldata1 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 구간구분

# COMMAND ----------

final2['playTime'].describe()

# COMMAND ----------

def get_interval(x):
  if x < 11 :
    return 10
  elif x < 21 :
    return 20
  elif x < 31 : 
    return 30
  elif x < 41:
    return 40
  elif x < 51:
    return 50
  elif x < 61:
    return 60
  else:
    return None

# COMMAND ----------

final2["playTimeinterval"] =""
final2['playTimeinterval'] = final2["playTime"].apply(lambda x : get_interval(x))

# COMMAND ----------

final2['playTimeinterval']

# COMMAND ----------

print(final2[final2['playTimeinterval'] ==10]) #90
print(final2[final2['playTimeinterval'] ==20])
print(final2[final2['playTimeinterval'] ==30])
print(final2[final2['playTimeinterval'] ==40])
print(final2[final2['playTimeinterval'] ==50]) #38
print(final2[final2['playTimeinterval'] ==60]) #4542       

# COMMAND ----------

final2

# COMMAND ----------

spark.sql("select * from final_df order by playTime").display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

final2 = final.select("*").toPandas()

# COMMAND ----------

def get_int(x):
  if final2['playTime'] < 11 :
    return 
  elif final2['playTime'] < 15 :
    return
  elif final2['playTime'] < 20 :
    return
  elif final2['playTime'] < 30:

# COMMAND ----------

final2['playtime2'] = ""
final2['playtime2'] = final2['playTime'].apply(lambda x : get_int(x))