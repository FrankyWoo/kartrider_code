# Databricks notebook source
import pandas as pd
df = pd.read_csv("/dbfs/FileStore/KartRider/2/3/matchinfo_total_df.csv", header='infer') 
matchinfo_total_df.drop('Unnamed: 0',axis=1,inplace=True)
matchinfo_total_df["p_matchTime"]=matchinfo_total_df["p_matchTime"]/1000

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/8_9_processed_df.csv"))

# COMMAND ----------

final.registerTempTable("dfsql")

# COMMAND ----------

track2 = spark.read.json("/FileStore/KartRider/track.json")
track2.registerTempTable("ttt")

# COMMAND ----------

#트랙정보 불러오기
kart_meta = spark.read.json("/FileStore/KartRider/2/3/kart.json")
kart_meta.registerTempTable("kart_meta")
track_meta = spark.read.json("/FileStore/KartRider/2/3/track_detail.json")
track_meta.registerTempTable("track_meta")
track_info=spark.read.json("/FileStore/KartRider/2/3/track_id.json")
track_info.registerTempTable("track_info")

# COMMAND ----------

track_meta.display()

# COMMAND ----------

spark.sql("select * from track_info").display()

# COMMAND ----------

track_info.groupBy('id').count().display()

# COMMAND ----------

track_info = track_info.withColumnRenamed('id', 'trackId')

# COMMAND ----------

track_info = track_info.withColumnRenamed('name', 'trackname')

# COMMAND ----------

track_info.display()

# COMMAND ----------

output = kart_join.join(track_info,['trackId'],how='inner')

# COMMAND ----------

output.display()

# COMMAND ----------

kart2 = kart_join
kart2.display()

# COMMAND ----------

kart2 = kart2.withColumn("trackname", lit(None)) 

# COMMAND ----------

kart2.display()

# COMMAND ----------

merged_df = kart2.unionByName(track_info, allowMissingColumns=True)

# COMMAND ----------

merged_df.display()

# COMMAND ----------

#카트정보합치기
kart_join= spark.sql("""
select a.*, b.name
from dfsql as a
left join kart_meta as b
on 1=1
and a.p_kart = b.id
""")

kart_join.registerTempTable("kart_join")

# COMMAND ----------

spark.sql("select * from kart_join").display()

# COMMAND ----------

#트랙이름만 조인
kart_track_join = spark.sql("""
select a.*, b.name as trackname
from kart_join as a
left join ttt as b
on 1=1
and a.trackId = b.id
""")
kart_track_join.display()
kart_track_join.registerTempTable("kart_track_join")

# COMMAND ----------

spark.sql("select trackname, count(trackname) from kart_track_join group by trackname").display()

# COMMAND ----------

#트랙정보 합치기
track_table=spark.sql("""
select a.*, b.*
from track_info as a
left join track_meta as b
on 1=1
and a.name = b.track
""")
track_table.display()
track_table.registerTempTable("track_tmp")

# COMMAND ----------

# 트랙정보 선별해서 다시 데이터프레임
track_df=spark.sql("""
select id, track, difficulty, distance, lap, mode from track_tmp
""")
track_df.registerTempTable("track_df")
track_df.display()


# COMMAND ----------

spark.sql("select * from track_meta").display()

# COMMAND ----------

spark.sql("select * from track_meta").display()

# COMMAND ----------

final_tmp=spark.sql("""
select *
from kart_track_join as a
left join track_meta as b
on 1=1 and a.trackname= b.track
""")

final_tmp.registerTempTable("total_tmp")

final_tmp.display()

# COMMAND ----------

#데이터프레임 csv로도 저장
import pyspark
from pyspark.sql.functions import *

final_tmp.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/lastone4.csv")


# COMMAND ----------

final_tmp.mode("overwrite").save("/my-output/default-csv")

# COMMAND ----------

.mode("overwrite").save("/my-output/default-csv")

# COMMAND ----------

final_tmp.write.csv("/dbfs/FileStore/KartRider/lastone3.csv")

# COMMAND ----------

final_tmp.write.csv("/dbfs:/FileStore/KartRider/lastone2.csv")
#/FileStore/KartRider/2/3/track_id.json
#final.write.csv("hdfs:///my/path/")

# COMMAND ----------

spark.sql("select track, count(track) from total_df group by track").display()