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

head(kart_tibble)

# COMMAND ----------

kart_lm <- lm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble)

# COMMAND ----------

summary(kart_lm)

# COMMAND ----------

### dataframe 형성
dataframe <- data.frame(label, second_column)

# COMMAND ----------

### cook's distance
cooksD <- cooks.distance(kart_lm)
cooksD

# COMMAND ----------

### cook's distance cutoff 
cd_cutoff <- mean(cooksD) * 3
cd_cutoff

# COMMAND ----------

### dataframe 
kart_tibble$cd_cutoff <- cd_cutoff
kart_tibble$cooks_distance <- cooksD

# COMMAND ----------

kart_tibble

# COMMAND ----------

#https://www.statisticshowto.com/cooks-distance/#:~:text=A%20general%20rule%20of%20thumb,is%20the%20number%20of%20observations.
library(dplyr)


depr_df <- depr_df %>% mutate(C = if_else(A == B, "Equal", "Not Equal"))


# Adding column based on other column:
kart_tibble %>%
  mutate(outlier = case_when(
    endsWith(cooks_distance, cooks_distance > cd_cutoff) ~ "Recovered",
    endsWith(cooks_distance, "S") ~ "Sick"
    ))

# COMMAND ----------

# Multiple conditions when adding new column to dataframe:
kart_tibble <- kart_tibble %>% mutate(outlier =
                     case_when(cooks_distance > cd_cutoff  ~ 1, 
                               cooks_distance <= cd_cutoff ~ 0)
)

# COMMAND ----------

head(kart_tibble)

# COMMAND ----------

kart_df_pyspark <- createDataFrame(kart_df)

# COMMAND ----------

createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select id, p_matchTime from kart_df

# COMMAND ----------

# MAGIC %sql 
# MAGIC select outlier, count(*) from kart_tibble group by outlier

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.sql("select * from kart_df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
# MAGIC df.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC df.count()

# COMMAND ----------

# %python
# df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versioA_cdTest.csv")

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

# COMMAND ----------

# MAGIC %python
# MAGIC sparkDF.registerTempTable("sparkDF")

# COMMAND ----------

# MAGIC %python 
# MAGIC sparkDF.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC sparkDF

# COMMAND ----------

# MAGIC %sql 
# MAGIC select a.id, a.outlier, b.label from kart_tibble as a left join sparkDF as b on 1=1 and a.id = b.id 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id 

# COMMAND ----------

results <- SparkR::sql('select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id')

# COMMAND ----------

typeof(results)

# COMMAND ----------

head(results)

# COMMAND ----------

library(caret)
library(InformationValue)

# COMMAND ----------

# MAGIC %python 
# MAGIC result = spark.sql("select b.id, b.outlier, a.label from sparkDF as a left join kart_tibble as b on 1=1 and a.id = b.id")
# MAGIC result.registerTempTable("result")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from result

# COMMAND ----------

# MAGIC %python 
# MAGIC result.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from result where outlier != label

# COMMAND ----------

sparkDF.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/versioA_labeld_test.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC final_cd = result.toPandas()

# COMMAND ----------

# MAGIC %python
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
# MAGIC from sklearn.metrics import f1_score
# MAGIC print('F1 Score: %.3f' % f1_score(final_cd['label'], final_cd['outlier']))

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.linear_model import LogisticRegression
# MAGIC from sklearn import metrics
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC #define metrics
# MAGIC 
# MAGIC fpr, tpr, _ = metrics.roc_curve(final_cd['label'], final_cd['outlier'])
# MAGIC 
# MAGIC #create ROC curve
# MAGIC plt.plot(fpr,tpr)
# MAGIC plt.ylabel('True Positive Rate')
# MAGIC plt.xlabel('False Positive Rate')
# MAGIC plt.show()

# COMMAND ----------

# MAGIC %python
# MAGIC #define metrics
# MAGIC 
# MAGIC fpr, tpr, _ = metrics.roc_curve(final_cd['label'], final_cd['outlier'])
# MAGIC auc = metrics.roc_auc_score(final_cd['label'], final_cd['outlier'])
# MAGIC 
# MAGIC #create ROC curve
# MAGIC plt.plot(fpr,tpr,label="AUC="+str(auc))
# MAGIC plt.ylabel('True Positive Rate')
# MAGIC plt.xlabel('False Positive Rate')
# MAGIC plt.legend(loc=4)
# MAGIC plt.show()

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import roc_curve
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

# COMMAND ----------

library(ROCR)

set.seed(3)
df <- data.frame(time      = c(2.5 + rnorm(5), 3.5 + rnorm(5)),
                 truth     = rep(c("cleaned", "final"), each = 5)) %>%
  mutate(predicted = if_else(time < 2.5, "cleaned", "final"))

pred <- prediction(df$time, df$truth)
perf <- performance(pred,"tpr","fpr")
plot(perf,colorize=TRUE)

# COMMAND ----------

# MAGIC %python
# MAGIC from yellowbrick.classifier import ROCAUC
# MAGIC  
# MAGIC 
# MAGIC visualizer = ROCAUC(xgb_basic, classes=[0, 1], micro=False, macro=True, per_class=False)
# MAGIC visualizer.fit(X_train, df["y"])
# MAGIC visualizer.score(X_train, df["y"])
# MAGIC visualizer.show()
# MAGIC  

# COMMAND ----------

df4 = spark.sql("""
select a.*, b.label
from df_tmp as a
left join kart_meta as b
on 1=1
and a.p_kart = b.id
""")

df4.registerTempTable("df4")

# COMMAND ----------

library(ggplot2)
results

# COMMAND ----------

d <- density(kart_tibble$p_matchTime)
plot(d, main="Kernel Density of matchtime")
polygon(d, col="red", border="blue")

# COMMAND ----------

# Add a Normal Curve (Thanks to Peter Dalgaard)
x <-kart_tibble$p_matchTime
h<-hist(x, breaks=10, col="red", xlab="matchitme",
   main="Histogram with Normal Curve")
xfit<-seq(min(x),max(x),length=40)
yfit<-dnorm(xfit,mean=mean(x),sd=sd(x))
yfit <- yfit*diff(h$mids[1:2])*length(x)
lines(xfit, yfit, col="blue", lwd=2)

# COMMAND ----------

log_matchtime <- kart_tibble$p_matchTime
hist(log_matchtime)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from kart_tibble

# COMMAND ----------

tmp <- createDataFrame(kart_tibble)

# COMMAND ----------

createOrReplaceTempView(tmp, "tmp")

# COMMAND ----------

# MAGIC %python
# MAGIC dataset = spark.sql("select * from tmp")

# COMMAND ----------

# MAGIC %python 
# MAGIC p_matchTime = dd["p_matchTime"].values
# MAGIC p_matchTime

# COMMAND ----------

# MAGIC %python 
# MAGIC f = Fitter(p_matchTime,distributions=['gamma','lognorm',"beta","burr","norm"])
# MAGIC f.fit()
# MAGIC f.summary()

# COMMAND ----------

# MAGIC %python
# MAGIC f.get_best(method = 'sumsquare_error')