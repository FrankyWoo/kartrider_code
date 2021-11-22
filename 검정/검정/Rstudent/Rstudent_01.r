# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/RegResult/cooks_distance.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

library(tidyverse)
kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()
head(kart_tibble)

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

kart_tibble

# COMMAND ----------

kart_lm <- lm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble)

# COMMAND ----------

kart_tibble$rstudent <- rstudent(kart_lm)
kart_tibble$rstudent

# COMMAND ----------

library(SparkR)
kart_df <- read.df( "/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

library(tidyverse)
kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()
head(kart_tibble)

# COMMAND ----------

data <- read.df("/FileStore/KartRider/RegResult/versionA_label.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")
data

# COMMAND ----------

kart_tibble

# COMMAND ----------

createOrReplaceTempView(data, "data")
#createOrReplaceTempView(kart_tibble, "kart_tibble")

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
# MAGIC df.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv")

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

df_answer <- SparkR::sql("select * from kart_tibble where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")

# COMMAND ----------

createOrReplaceTempView(df_answer, "df_answer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select percentile_approx(rstudent, 0.85, 100) q085,
# MAGIC        percentile_approx(rstudent, 0.90, 100) q090,
# MAGIC        percentile_approx(rstudent, 0.92, 100) q092,
# MAGIC        percentile_approx(rstudent, 0.93, 100) q093,
# MAGIC        percentile_approx(rstudent, 0.94, 100) q094,
# MAGIC        percentile_approx(rstudent, 0.95, 100) q095,
# MAGIC        percentile_approx(rstudent, 0.96, 100) q096,
# MAGIC        percentile_approx(rstudent, 0.965, 100) q0965,
# MAGIC        percentile_approx(rstudent, 0.97, 100) q097,
# MAGIC        percentile_approx(rstudent, 0.973, 100) q0973,
# MAGIC        percentile_approx(rstudent, 0.974, 100) q0974,
# MAGIC        percentile_approx(rstudent, 0.975, 100) q0975,
# MAGIC        percentile_approx(rstudent, 0.977, 100) q0977,
# MAGIC        percentile_approx(rstudent, 0.98, 100) q098,
# MAGIC        percentile_approx(rstudent, 0.985, 100) q0985,
# MAGIC        percentile_approx(rstudent, 0.99, 100) q099,
# MAGIC        percentile_approx(rstudent, 0.995, 100) q0995,
# MAGIC        percentile_approx(rstudent, 0.999, 100) q0999
# MAGIC from(
# MAGIC select a.id, a.rstudent 
# MAGIC from kart_tibble as a left join data as b on 1=1 and a.id = b.id
# MAGIC ) as t

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when Abs(a.rstudent) > 1.0809736803124876 then 1 else 0 end outlier, b.label as label
# MAGIC from df_answer as a left join data as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.count()

# COMMAND ----------

# MAGIC %python
# MAGIC #0.92
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #0.93
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #0.95
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #0.94
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC ### 0.94
# MAGIC from sklearn.metrics import classification_report
# MAGIC #panda = df_answer.toPandas()
# MAGIC print(classification_report(panda['label'], panda['outlier']))

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
# MAGIC roc_curve_plot(panda['label'], panda['outlier'])

# COMMAND ----------

from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix,roc_auc_score

print("f1_score:"+str(f1_score(labeled["label"].astype(str), labeled["anomal_clustered_label0.075"].astype(str), pos_label=1,average="macro")))
print("precision:"+str(precision_score(labeled["label"].astype(str), labeled["anomal_clustered_label0.075"].astype(str),pos_label=1, average="macro")))
print("recall:"+str(recall_score(labeled["label"].astype(str), labeled["anomal_clustered_label0.075"].astype(str),pos_label=1, average="macro")))
print("accuracy:"+str(accuracy_score(labeled["label"].astype(str), labeled["anomal_clustered_label0.075"].astype(str))))
print("roc_auc:"+str(roc_auc_score(labeled["label"].astype(str), labeled["anomal_clustered_label0.075"].astype(str))))

# COMMAND ----------


plot(kart_tibble$rstudent , 
     xlab="Index",
     ylab="studentized residuals",
     pch=19)
abline(h = 1.0809736803124876, col="red")  # add cutoff line
abline(h = -1.0809736803124876, col="red")  # add cutoff line


# COMMAND ----------

# MAGIC %sql 
# MAGIC select dffits, cooks_distance, rstudent from kart_tibble

# COMMAND ----------

# MAGIC %python 
# MAGIC tmp = spark.sql("select dffits, cooks_distance, rstudent from kart_tibble ")