# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA_id.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

kart_df

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

head(kart_tibble)

# COMMAND ----------

kart_lm <- lm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble)

# COMMAND ----------

summary(kart_lm)

# COMMAND ----------

d <- dffits(kart_lm)

d

# COMMAND ----------

dffits

# COMMAND ----------

dffits <- as.data.frame(dffits(kart_lm))
dffits

# COMMAND ----------

rdf <- tibble::rowid_to_column(dffits, "id")
names(rdf)[names(rdf) == " dffits(kart_lm)"] <- "dffits"

# COMMAND ----------

dffits['dffits(kart_lm)'][1,]

# COMMAND ----------

#find number of predictors in model
p <- length(kart_lm$coefficients)-1

#find number of observations
n <- nrow(kart_df)

#calculate DFFITS threshold value
thresh <- 2*sqrt(p/n)

thresh

# COMMAND ----------

### dataframe 
kart_tibble$d_cutoff <- thresh
kart_tibble$dffits <- d

# COMMAND ----------

# Multiple conditions when adding new column to dataframe:
kart_tibble <- kart_tibble %>% mutate(outlier =
                     case_when(dffits > d_cutoff  ~ 1, 
                               dffits <= d_cutoff ~ 0)
)

# COMMAND ----------

kart_tibble_pyspark <- createDataFrame(kart_tibble)

# COMMAND ----------

createOrReplaceTempView(kart_tibble_pyspark, "kart_tibble")

# COMMAND ----------

# MAGIC %python
# MAGIC df2=spark.sql("select * from kart_tibble where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
# MAGIC df2.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC df2.count()

# COMMAND ----------

# MAGIC %python
# MAGIC df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versioA_diTest.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC sample_pd=df.toPandas()
# MAGIC sample_pd

# COMMAND ----------

createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.sql("select * from kart_df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
# MAGIC df.display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC sample_pd=df.toPandas()
# MAGIC sample_pd

# COMMAND ----------

# MAGIC %python
# MAGIC 
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

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC concated=pd.concat([sample_brody,sample_china_rush,sample_china_dragon,sample_villeage,sample_basement])
# MAGIC concated

# COMMAND ----------

# MAGIC %python
# MAGIC print(concated)
# MAGIC concated['label'].value_counts()

# COMMAND ----------

# MAGIC %python
# MAGIC sparkDF=spark.createDataFrame(concated) 
# MAGIC sparkDF.registerTempTable("sparkDF")
# MAGIC sparkDF.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id 

# COMMAND ----------

results <- SparkR::sql('select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id')

# COMMAND ----------

# MAGIC %python
# MAGIC result = spark.sql("select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id")
# MAGIC result.registerTempTable("result")
# MAGIC final_cd = result.toPandas()
# MAGIC type(final_cd['outlier'])
# MAGIC final_cd

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
# MAGIC get_clf_eval(final_cd['label'], final_cd['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC final_cd['label'].value_counts()

# COMMAND ----------

# MAGIC %python
# MAGIC final_cd['outlier'].value_counts()

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import f1_score
# MAGIC print('F1 Score: %.3f' % f1_score(final_cd['label'], final_cd['outlier']))

# COMMAND ----------

results <- SparkR::sql('select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id')

# COMMAND ----------

nrow(results)

# COMMAND ----------

library(pROC)


prob <- predict(kart_lm,type=c("response"))

kart_lm$prob <- prob

roc1 <- roc(p_matchTime ~ prob, data = kart_tibble, plot = FALSE)

roc1

# COMMAND ----------

plot(roc1, lty = "solid")

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import roc_curve
# MAGIC import numpy as np
# MAGIC 
# MAGIC 
# MAGIC # 레이블 값이 1일때의 예측확률을 추출
# MAGIC #pred_positive_label = lr_model.predict_proba(X_test)[:,1]
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC fprs, tprs, thresholds = roc_curve(final_cd['label'], final_cd['outlier'])
# MAGIC 
# MAGIC 
# MAGIC print()
# MAGIC 
# MAGIC thr_idx = np.arange(1,thresholds.shape[0],6)
# MAGIC 
# MAGIC print('thr idx:',thr_idx)
# MAGIC 
# MAGIC print('thr thresholds value:',thresholds[thr_idx])
# MAGIC 
# MAGIC print('thr thresholds value:',fprs[thr_idx])
# MAGIC 
# MAGIC print('thr thresholds value:',tprs[thr_idx])

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.linear_model import LogisticRegression
# MAGIC from sklearn import metrics
# MAGIC import matplotlib.pyplot as plt
# MAGIC fprs, tprs, thresholds = roc_curve(final_cd['label'], final_cd['outlier'])
# MAGIC 
# MAGIC precisions, recalls, thresholds = roc_curve(final_cd['label'], final_cd['outlier'])
# MAGIC plt.figure(figsize=(15,5))
# MAGIC 
# MAGIC # 대각선
# MAGIC 
# MAGIC plt.plot([0,1],[0,1],label='STR')
# MAGIC 
# MAGIC 
# MAGIC # ROC
# MAGIC 
# MAGIC plt.plot(fprs,tprs,label='ROC')
# MAGIC 
# MAGIC 
# MAGIC plt.xlabel('FPR')
# MAGIC 
# MAGIC plt.ylabel('TPR')
# MAGIC 
# MAGIC plt.legend()
# MAGIC 
# MAGIC plt.grid()
# MAGIC 
# MAGIC plt.show()

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import roc_auc_score
# MAGIC 
# MAGIC from sklearn.linear_model import LogisticRegression
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # # 위 코드 확인 후
# MAGIC 
# MAGIC # # 로지스틱 회귀
# MAGIC 
# MAGIC # lr_model = LogisticRegression()
# MAGIC 
# MAGIC # lr_model.fit(X_train,y_train)
# MAGIC 
# MAGIC # prediction = lr_model.predict(X_test)
# MAGIC 
# MAGIC print('roc auc value {}'.format(roc_auc_score(final_cd['label'], final_cd['outlier']))) # 이  value는 auc에 대한 면적을 나타낸 것이다.