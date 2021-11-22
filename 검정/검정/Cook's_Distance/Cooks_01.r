# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/RegResult/dffits.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

library(tidyverse)
kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()
head(kart_tibble)
# kart_tibble <- kart_tibble %>%
#   mutate(p_matchTime = log(p_matchTime),
#          channelName = as.factor(channelName),
#          p_rankinggrade2 = as.factor(p_rankinggrade2),
#          teamPlayers = as.factor(teamPlayers),
#          track  = as.factor(track),
#          gameSpeed = as.factor(gameSpeed))

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

kart_tibble

# COMMAND ----------

kart_lm <- lm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble)

# COMMAND ----------

kart_tibble$cooks_distance <- cooks.distance(kart_lm)
kart_tibble$cooks_distance

# COMMAND ----------

kart_tibble

# COMMAND ----------

plot(kart_tibble$cooks_distance, pch="*", cex=2, main="Influential Obs by Cooks distance")  
abline(h = 3.861943275329788e-7, col="red")  # add cutoff line
#text(x=1:length(cd), y=cd, labels=ifelse(cd>0.011375837,names(cd),""), col="red", pos = 4)  # add labels # pos = 4 to position at the right 

# Or alternatively,
#identify(1:length(cd),cooks.distance(lm3),row.names(outliers)) # interactive labeling, LOVE it! :)

# COMMAND ----------

kart_tibble_pyspark <- createDataFrame(kart_tibble)
createOrReplaceTempView(kart_tibble_pyspark, "kart_tibble")

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.sql("select * from kart_tibble")
# MAGIC print(df.count())
# MAGIC print(df.display())

# COMMAND ----------

# MAGIC %python
# MAGIC df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/cooks_distance.csv")

# COMMAND ----------

data <- read.df("/FileStore/KartRider/versionA_id.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

createOrReplaceTempView(data, "data")

# COMMAND ----------

# MAGIC %python 
# MAGIC ### p_matchTime 원데이터 확인 
# MAGIC eval =spark.sql("select * from data where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
# MAGIC eval.display()

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC sample_pd=eval.toPandas()
# MAGIC sample_brody=sample_pd[sample_pd["track"]=="브로디 비밀의 연구소"]
# MAGIC sample_brody["label"] = [0 if  181>= x >=122 else 1 for x in sample_brody["p_matchTime"]]
# MAGIC 
# MAGIC sample_china_rush=sample_pd[sample_pd["track"]=="차이나 골목길 대질주"]
# MAGIC sample_china_rush["label"] = [0 if  182>= x >=123 else 1 for x in sample_china_rush["p_matchTime"]]
# MAGIC 
# MAGIC sample_china_dragon=sample_pd[sample_pd["track"]=="차이나 용의 운하"]
# MAGIC sample_china_dragon["label"] = [0 if  179>= x >=120 else 1 for x in sample_china_dragon["p_matchTime"]]
# MAGIC 
# MAGIC sample_villeage=sample_pd[sample_pd["track"]=="빌리지 운명의 다리"]
# MAGIC sample_villeage["label"] = [0 if  176>= x >=117 else 1 for x in sample_villeage["p_matchTime"]]
# MAGIC 
# MAGIC sample_basement=sample_pd[sample_pd["track"]=="대저택 은밀한 지하실"]
# MAGIC sample_basement["label"] = [0 if  176>= x >=117 else 1 for x in sample_basement["p_matchTime"]]
# MAGIC 
# MAGIC concated=pd.concat([sample_brody,sample_china_rush,sample_china_dragon,sample_villeage,sample_basement])
# MAGIC concated

# COMMAND ----------

# MAGIC %python
# MAGIC sparkDF=spark.createDataFrame(concated)
# MAGIC sparkDF.registerTempTable("sparkDF")
# MAGIC sparkDF.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/versionA_label.csv")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from kart_tibble as a left join sparkDF as b on 1=1 and a.id = b.id

# COMMAND ----------

kart_tibble2 <- SparkR::sql("select * from kart_tibble where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")

# COMMAND ----------

createOrReplaceTempView(kart_tibble2, "kart_tibble2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.id, a.cooks_distance,  b.label 
# MAGIC from kart_tibble2 as a left join sparkDF as b on 1=1 and a.id = b.id

# COMMAND ----------

quantile(kart_tibble$cooks_distance)

# COMMAND ----------

### cook's distance cutoff 
## large_cd<-subset(outliers, cd > (4/1964)) 
cd_cutoff <- mean(kart_tibble$cooks_distance) * 3
cd_cutoff

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

cd_cutoff2 <- 4 / 1251609
cd_cutoff2

# COMMAND ----------

large_cd<-subset(kart_tibble$cooks_distance, kart_tibble$cooks_distance > (4/1251609)) 
large_cd

# COMMAND ----------

length(large_cd)

# COMMAND ----------

hist(large_cd)

# COMMAND ----------

### cook's distance
library(Hmisc)
describe(large_cd)

# COMMAND ----------

#0.00000319588
quantile(large_cd, probs = seq(0, 1, 0.05)) 

# COMMAND ----------

### cook's distance cutoff 
cd_cutoff <- mean(kart_tibble$cooks_distance) * 3
cd_cutoff

# COMMAND ----------

# MAGIC %python 
# MAGIC #final_spark = spark.createDataFrame(final_answer)
# MAGIC #final_spark = spark.createDataFrame(df_answer_diff)
# MAGIC final_spark.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/pre_test_dffits.csv")

# COMMAND ----------

print(4 / 1251609)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer = spark.sql("""
# MAGIC select a.id as id, case when a.cooks_distance > 3.195886e-06 then 1 else 0 end cooks_outlier, b.label as label
# MAGIC from kart_tibble2 as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer.display()

# COMMAND ----------

# MAGIC %python
# MAGIC # panda = df_answer.toPandas()
# MAGIC # cutoff : 4/n 0.00000319588
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer = spark.sql(""" 
# MAGIC select a.id as id, case when a.cooks_distance > 0.00000389184 then 1 else 0 end cooks_outlier, b.label as label
# MAGIC from kart_tibble2 as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC # cutoff : mean*3
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())

# COMMAND ----------

# MAGIC %python
# MAGIC #0.00002491911
# MAGIC df_answer = spark.sql(""" 
# MAGIC select a.id as id, case when a.cooks_distance > 0.00002491911 then 1 else 0 end cooks_outlier, b.label as label
# MAGIC from kart_tibble2 as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)
# MAGIC df_answer.registerTempTable(df_answer)

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #0.00002491911
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())

# COMMAND ----------

# MAGIC %sql
# MAGIC select percentile_approx(cooks_distance, 0.85, 100) q085,
# MAGIC        percentile_approx(cooks_distance, 0.90, 100) q090,
# MAGIC        percentile_approx(cooks_distance, 0.95, 100) q095,
# MAGIC        percentile_approx(cooks_distance, 0.96, 100) q096,
# MAGIC        percentile_approx(cooks_distance, 0.965, 100) q0965,
# MAGIC        percentile_approx(cooks_distance, 0.97, 100) q097,
# MAGIC        percentile_approx(cooks_distance, 0.973, 100) q0973,
# MAGIC        percentile_approx(cooks_distance, 0.974, 100) q0974,
# MAGIC        percentile_approx(cooks_distance, 0.975, 100) q0975,
# MAGIC        percentile_approx(cooks_distance, 0.977, 100) q0977,
# MAGIC        percentile_approx(cooks_distance, 0.98, 100) q098,
# MAGIC        percentile_approx(cooks_distance, 0.985, 100) q0985,
# MAGIC        percentile_approx(cooks_distance, 0.99, 100) q099,
# MAGIC        percentile_approx(cooks_distance, 0.995, 100) q0995,
# MAGIC        percentile_approx(cooks_distance, 0.999, 100) q0999
# MAGIC from(
# MAGIC select a.id, a.cooks_distance
# MAGIC from kart_tibble2 as a ) as t

# COMMAND ----------

8.335240378666423e-7

# COMMAND ----------

8.335240378666423e-7 < 8.335240378666423e-5

# COMMAND ----------

# MAGIC %python
# MAGIC #0.00002491911
# MAGIC df_answer = spark.sql(""" 
# MAGIC select a.id as id, case when a.cooks_distance > 3.861943275329788e-7 then 1 else 0 end cooks_outlier, b.label as label
# MAGIC from kart_tibble2 as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)
# MAGIC df_answer.registerTempTable('df_answer')

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #8.335240378666423e-7 
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix
# MAGIC 
# MAGIC def get_clf_eval(y_test, pred):
# MAGIC     confusion = confusion_matrix(y_test, pred)
# MAGIC     accuracy = accuracy_score(y_test, pred)
# MAGIC     precision = precision_score(y_test, pred)
# MAGIC     recall = recall_score(y_test, pred)
# MAGIC     print('Confusion Matrix')
# MAGIC     print(confusion)
# MAGIC     print('정확도:{}, 정밀도:{}, 재현율:{}'.format(accuracy, precision, recall))

# COMMAND ----------

# MAGIC %python
# MAGIC #q : 0.985
# MAGIC get_clf_eval(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #q : 0.99
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC 
# MAGIC get_clf_eval(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #q : 0.98
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC 
# MAGIC get_clf_eval(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #q : 0.97
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC 
# MAGIC get_clf_eval(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #q : 0.965
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC 
# MAGIC get_clf_eval(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC panda = df_answer.toPandas()
# MAGIC #q : 0.96
# MAGIC print(panda['cooks_outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC 
# MAGIC get_clf_eval(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC ### q : 0.965
# MAGIC from sklearn.metrics import classification_report
# MAGIC panda = df_answer.toPandas()
# MAGIC print(classification_report(panda['label'], panda['cooks_outlier']))

# COMMAND ----------

# MAGIC %python
# MAGIC import matplotlib.pyplot as plt
# MAGIC from sklearn.metrics import roc_curve
# MAGIC 
# MAGIC def roc_curve_plot(y_test, pred_proba_c1):
# MAGIC     # 임계값에 따른 FPR, TPR 값
# MAGIC     fprs, tprs, thresholds = roc_curve(y_test, pred_proba_c1)
# MAGIC     # ROC 곡선을 시각화
# MAGIC     plt.plot(fprs, tprs, label='ROC')
# MAGIC     # 가운데 대각선 직선
# MAGIC     plt.plot([0,1],[0,1], 'k--',label='Random')
# MAGIC     
# MAGIC     # FPR X 축의 scla 0.1 단위 지정
# MAGIC     start, end = plt.xlim()
# MAGIC    # plt.xticks(np.round(np.arange(start, end, 0.1), 2))
# MAGIC     plt.xlim(0, 1)
# MAGIC     plt.ylim(0, 1)
# MAGIC     plt.xlabel('FPR( 1 - Snesitivity )')
# MAGIC     plt.ylabel('TPR( Recall )')
# MAGIC     plt.legend()
# MAGIC     
# MAGIC roc_curve_plot(panda['label'], panda['cooks_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC spa = spark.sql("select * from kart_tibble2")
# MAGIC spa_pandas = spa.toPandas()

# COMMAND ----------

# MAGIC %python
# MAGIC import plotly.express as px
# MAGIC fig = px.scatter(spa_pandas, x="id", y="cooks_distance")
# MAGIC fig.show()

# COMMAND ----------

# MAGIC %python
# MAGIC concated