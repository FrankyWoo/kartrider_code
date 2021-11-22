# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA_id.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

library(tidyverse)

# COMMAND ----------

kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()

# COMMAND ----------

kart_tibble <- kart_tibble %>%
  mutate(p_matchTime = log(p_matchTime),
         channelName = as.factor(channelName),
         p_rankinggrade2 = as.factor(p_rankinggrade2),
         teamPlayers = as.factor(teamPlayers),
         track  = as.factor(track),
         gameSpeed = as.factor(gameSpeed))

# COMMAND ----------

kart_lm <- lm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble)

# COMMAND ----------

summary(kart_lm)

# COMMAND ----------

AIC(kart_lm)

# COMMAND ----------

BIC(kart_lm)

# COMMAND ----------

### save(kart_lm, file="/dbfs/FileStore/model/ols_model.RData")  

# COMMAND ----------

# MAGIC %md 
# MAGIC dffits 

# COMMAND ----------

kart_tibble$dffits <- dffits(kart_lm)

# COMMAND ----------

kart_tibble

# COMMAND ----------

### copy kart_tibble 
df <- kart_tibble

# COMMAND ----------

df

# COMMAND ----------

### dffits 값 확인 
kart_tibble$dffits

# COMMAND ----------

df$dffits

# COMMAND ----------

kart_tibble_pyspark <- createDataFrame(kart_tibble)
createOrReplaceTempView(kart_tibble_pyspark, "kart_tibble")

# COMMAND ----------

# MAGIC %python 
# MAGIC kart = spark.sql("select * from kart_tibble")
# MAGIC kart.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC kart.count()

# COMMAND ----------

# MAGIC %python
# MAGIC #kart.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/dffits.csv")

# COMMAND ----------

### threshold 지정 
threshold <- 0.03107071

### 0.003542803940456012


# COMMAND ----------

df <- df %>% mutate(dffit_outlier =
                     case_when(abs(dffits) > threshold  ~ 1, 
                               abs(dffits) <= threshold ~ 0)
)

# COMMAND ----------

df$dffit_outlier

# COMMAND ----------

2*sqrt((p+1)/(n-p+1))

# COMMAND ----------

# ###threshold : 0.03096394
# #find number of predictors in model
# p <- length(kart_lm$coefficients)-1

# #find number of observations
# n <- nrow(kart_df)

# #calculate DFFITS threshold value
# thresh <- 2*sqrt(p/n)

# thresh

# COMMAND ----------

n<-nrow(kart_tibble)
p<-length(coef(kart_lm))

# COMMAND ----------

p

# COMMAND ----------

# MAGIC %md
# MAGIC threshold = 0.03107071

# COMMAND ----------

kart_tibble[which(abs(kart_tibble$dffits)>2*sqrt((p+1)/(n-p+1))),]

# COMMAND ----------

kart_tibble$dffits

# COMMAND ----------

kart_tibble$dffits[1] 

# COMMAND ----------

kart_tibble$dffit_outlier <- kart_tibble %>% mutate(dffit_outlier =
                     case_when(abs(dffits) > 0.03107071  ~ 1, 
                               abs(dffits) <= 0.03107071 ~ 0)
)

# COMMAND ----------

library(dplyr) #잘 분배되었는 지 확인 (1차)
dplyr::count(df, dffit_outlier, sort = TRUE)

# COMMAND ----------

kart_tibble$outlier

# COMMAND ----------

### sql table로 저장
df_pyspark <- createDataFrame(df)
createOrReplaceTempView(df_pyspark, "df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df where dffit_outlier = 1

# COMMAND ----------

# MAGIC %md
# MAGIC dffit_outlier 이용 ### 답지 생성 

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer = spark.sql("select * from df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
# MAGIC df_answer.display()

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer.count()

# COMMAND ----------

# MAGIC %python 
# MAGIC df_answer.columns

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer.groupBy('dffit_outlier').count().orderBy('count').display()

# COMMAND ----------

###kart_df 변환
typeof(kart_df)

# COMMAND ----------

createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

# MAGIC %python 
# MAGIC eval =spark.sql("select * from kart_df")
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
# MAGIC concated['label'].value_counts()

# COMMAND ----------

# MAGIC %python 
# MAGIC ### type 확인
# MAGIC type(concated['dffit_outlier'][0])

# COMMAND ----------

# MAGIC %python 
# MAGIC ### type 확인
# MAGIC type(concated['dffit_outlier'][0])

# COMMAND ----------

# MAGIC %python 
# MAGIC ### type 확인
# MAGIC concated['dffit_outlier']

# COMMAND ----------

# MAGIC %python 
# MAGIC concated['label'].value_counts()

# COMMAND ----------

# MAGIC %python
# MAGIC sparkDF=spark.createDataFrame(concated) 
# MAGIC sparkDF.registerTempTable("sparkDF")
# MAGIC sparkDF.show()

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer.registerTempTable("df_answer")

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/dffit_final.csv")

# COMMAND ----------

library(SparkR)
df_answer <- read.df("/FileStore/KartRider/dffit_final.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

createOrReplaceTempView(df_answer, "df_answer")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_answer

# COMMAND ----------

nrow(df_answer)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_answer

# COMMAND ----------

# MAGIC %sql 
# MAGIC select a.id, a.dffit_outlier, b.label from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id

# COMMAND ----------

# MAGIC %python
# MAGIC result = spark.sql("select a.id, a.dffit_outlier, b.label from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id")
# MAGIC result.registerTempTable("result")
# MAGIC final = result.toPandas()
# MAGIC final

# COMMAND ----------

select percentile_approx(dffits, 0.85, 100) dffits_q085,
       percentile_approx(dffits, 0.90, 100) dffits_q090,
       percentile_approx(dffits, 0.95, 100) dffits_q095,
       percentile_approx(dffits, 0.97, 100) dffits_q097,
       percentile_approx(dffits, 0.99, 100) dffits_q099,
       percentile_approx(dffits, 0.995, 100) dffits_q0995,
       percentile_approx(dffits, 0.999, 100) dffits_q0999
from(
select a.id, a.dffits, a.dffit_outlier, b.label 
from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
) as t

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.008781155177182528 then 1 else 0 end dffit_outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC final = df_answer_diff.toPandas()
# MAGIC final['dffit_outlier'] = final['dffit_outlier'].astype(int)

# COMMAND ----------

# MAGIC %python
# MAGIC final

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
# MAGIC get_clf_eval(final['label'], final['dffit_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC ### 0.85
# MAGIC get_clf_eval(final['label'], final['dffit_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC ### 0.95
# MAGIC get_clf_eval(final['label'], final['dffit_outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC ### 0.97
# MAGIC get_clf_eval(final['label'], final['dffit_outlier'])

# COMMAND ----------

# test <- SparkR::sql('select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id')

# COMMAND ----------

kart_tibble$dffit_outlier

# COMMAND ----------

count(kart_tibble[which(abs(kart_tibble$dffits)>2*sqrt((p+1)/(n-p+1))),])