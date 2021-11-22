# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/RegResult/cooks_distance.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

data <- read.df("/FileStore/KartRider/RegResult/versionA_label.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

createOrReplaceTempView(data, "data")
createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from kart_df

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from data

# COMMAND ----------

print(nrow(kart_df))
print(nrow(data))

# COMMAND ----------

# MAGIC %sql
# MAGIC select percentile_approx(dffits, 0.85, 100) dffits_q085,
# MAGIC        percentile_approx(dffits, 0.89, 100) dffits_q089,
# MAGIC        percentile_approx(dffits, 0.90, 100) dffits_q090,
# MAGIC        percentile_approx(dffits, 0.91, 100) dffits_q091,
# MAGIC        percentile_approx(dffits, 0.92, 100) dffits_q092,
# MAGIC        percentile_approx(dffits, 0.93, 100) dffits_q093,
# MAGIC        percentile_approx(dffits, 0.95, 100) dffits_q095,
# MAGIC        percentile_approx(dffits, 0.965, 100) dffits_q0965,
# MAGIC        percentile_approx(dffits, 0.97, 100) dffits_q097,
# MAGIC        percentile_approx(dffits, 0.973, 100) dffits_q0973,
# MAGIC        percentile_approx(dffits, 0.974, 100) dffits_q0974,
# MAGIC        percentile_approx(dffits, 0.975, 100) dffits_q0975,
# MAGIC        percentile_approx(dffits, 0.977, 100) dffits_q0977,
# MAGIC        percentile_approx(dffits, 0.98, 100) dffits_q098,
# MAGIC        percentile_approx(dffits, 0.985, 100) dffits_q0985,
# MAGIC        percentile_approx(dffits, 0.99, 100) dffits_q099,
# MAGIC        percentile_approx(dffits, 0.995, 100) dffits_q0995,
# MAGIC        percentile_approx(dffits, 0.999, 100) dffits_q0999
# MAGIC from(
# MAGIC select a.id, a.dffits 
# MAGIC from kart_df as a left join data as b on 1=1 and a.id = b.id
# MAGIC ) as t

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from kart_df

# COMMAND ----------



# COMMAND ----------

df_answer <- SparkR::sql("select * from kart_df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")

# COMMAND ----------

createOrReplaceTempView(df_answer, "df_answer")

# COMMAND ----------

# MAGIC %python 
# MAGIC df_answer.display()

# COMMAND ----------

# MAGIC %python
# MAGIC #0.011141143149623079
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when Abs(a.dffits) >= 0.011141143149623079 then 1 else 0 end outlier, b.label as label
# MAGIC from df_answer as a left join data as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.count()

# COMMAND ----------

# MAGIC %python
# MAGIC #0.985
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #0.0086705960774002  
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix,roc_auc_score
# MAGIC 
# MAGIC def get_clf_eval(y_test, pred):
# MAGIC     confusion = confusion_matrix(y_test, pred)
# MAGIC     accuracy = accuracy_score(y_test, pred)
# MAGIC     precision = precision_score(y_test, pred, average = "macro")
# MAGIC     recall = recall_score(y_test, pred, average = "macro")
# MAGIC     roc_auc = roc_auc_score(y_test, pred, average = "macro")
# MAGIC     f1score = f1_score(y_test, pred, average = "macro")
# MAGIC     print('Confusion Matrix')
# MAGIC     print(confusion)
# MAGIC     print('정확도:{}, 정밀도:{}, 재현율:{}, roc_auc:{}, f1_score:{}'.format(accuracy, precision, recall, roc_auc, f1score))
# MAGIC     
# MAGIC     

# COMMAND ----------

# MAGIC %python
# MAGIC #0.024120449628973757 
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #0.024120449628973757 
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #0.017858651894505008 
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.89
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.90
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.95
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.93
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.92 : 0.011141143149623079
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.92
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC #q:0.93
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

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
# MAGIC ###q:0.92
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

# MAGIC %python
# MAGIC from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix,roc_auc_score
# MAGIC 
# MAGIC def get_clf_eval(y_test, pred):
# MAGIC     confusion = confusion_matrix(y_test, pred)
# MAGIC     accuracy = accuracy_score(y_test, pred)
# MAGIC     precision = precision_score(y_test, pred)
# MAGIC     recall = recall_score(y_test, pred)
# MAGIC     roc_auc = roc_auc_score(y_test, pred)
# MAGIC     print('Confusion Matrix')
# MAGIC     print(confusion)
# MAGIC     print('정확도:{}, 정밀도:{}, 재현율:{}, roc_auc:{}'.format(accuracy, precision, recall, roc_auc))

# COMMAND ----------

# MAGIC %python
# MAGIC #0.94
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])