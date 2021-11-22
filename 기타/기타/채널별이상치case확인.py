# Databricks notebook source
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv"))

final.registerTempTable("final_df")

# COMMAND ----------

aaa = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv/part-00002-tid-4193259757182748304-663fd666-e254-4cf8-9980-826874b1f1a8-222-1-c000.csv"))


# COMMAND ----------

print((aaa.count(), len(aaa.columns)))

# COMMAND ----------

print((final.count(), len(final.columns)))

# COMMAND ----------

temp = final.toPandas()

# COMMAND ----------

temp 

# COMMAND ----------

temp.to_csv('/dbfs/FileStore/KartRider/tempdf.csv')

# COMMAND ----------

dbfs cp dbfs:/FileStore/KartRider/tempdf.csv 

# COMMAND ----------

#final.to_csv('/dbfs/FileStore/KartRider/tempdf.csv')
temp = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/tempdf.csv"))



# COMMAND ----------

# MAGIC %md
# MAGIC step 01 : 이상 탐지 발견한 채널 (speedTeamFastNewbie & speedTeamNewbie ) 데이터 확인 => 없다고 나옴. 이전 EDA 결과가 맞지 않다 판단. step02에서 피벗테이블 활용하여 다시 확인할 예정

# COMMAND ----------

#step01 : 채널별 이상탐지 케이스 확인 
final.display()

# COMMAND ----------

print(final.schema["p_rankinggrade2"].dataType)

# COMMAND ----------

final.filter(final.channelName == "speedTeamFastNewbie").display()

# COMMAND ----------

spark.sql("select * from final_df")

# COMMAND ----------

ab = spark.sql("select * from final_df where (channelName = 'speedTeamNewbie' and p_rankinggrade2 > 3)") # 총 120개 존재. 
ab.show()

# COMMAND ----------

type(ab)

# COMMAND ----------

#speedTeamNewbie 채널에 존재하는 유저 정보. 
newbieab= final.select("p_accountNo").rdd.flatMap(lambda x: x).collect()
newbieab

# COMMAND ----------

finaltmp2 = finaltmp.loc[finaltmp['p_accountNo'].isin(newbieab)]

# COMMAND ----------

finaltmp2

# COMMAND ----------

finaltmp = final.toPandas()N

# COMMAND ----------

spark.sql("select avg(playtime), STD(playtime), VAR_POP(playtime) from final_df where (channelName = 'speedTeamNewbie' and p_rankinggrade2 < 4)").display()

# COMMAND ----------

tmp2 = spark.sql("select playtime from final_df where (channelName = 'speedTeamNewbie' and p_rankinggrade2 < 4)")
tmp2.display()

# COMMAND ----------

import matplotlib
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
from IPython.display import HTML
import plotly.express as px
#rom plotly.offline.offline import _plot_html
import numpy as np
import cufflinks as cf

# COMMAND ----------

tmp1 = spark.sql("select playtime from final_df where (channelName = 'speedTeamNewbie' and p_rankinggrade2 < 4)")
tmp1.describe().show()

# COMMAND ----------

tmp1_tmp = tmp1.select("*").toPandas()

# COMMAND ----------

#이상치 값 확인 : playtime = 3868
fig = px.box(tmp1_tmp, y="playtime",  width=400, height=800)
fig.show()