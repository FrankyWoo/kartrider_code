# Databricks notebook source
library(SparkR)
df_answer <- read.df("/FileStore/KartRider/dffit_final.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

head(df_answer)

# COMMAND ----------

kart_df <- read.df("/FileStore/KartRider/versionA_id.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

createOrReplaceTempView(df_answer, "df_answer")
createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

print(nrow(df_answer))
print(nrow(kart_df))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_answer order by id asc

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from df_answer order by id asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_answer where id = 192704

# COMMAND ----------

# MAGIC %python 
# MAGIC ### p_matchTime 원데이터 확인 
# MAGIC eval =spark.sql("select * from kart_df where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
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
# MAGIC print(concated['label'].value_counts())

# COMMAND ----------

# MAGIC %python
# MAGIC sparkDF=spark.createDataFrame(concated)
# MAGIC sparkDF.registerTempTable("sparkDF")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sparkDF

# COMMAND ----------

# MAGIC %sql
# MAGIC select percentile_approx(dffits, 0.85, 100) dffits_q085,
# MAGIC        percentile_approx(dffits, 0.90, 100) dffits_q090,
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
# MAGIC select a.id, a.dffits, a.dffit_outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC ) as t

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.0086705960774002 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.display()

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
# MAGIC #97%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.009579304328198089 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC #97.5%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.00923121874821053 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC #97.3%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.009409549495580571 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC #97.3%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------

0.009982964140407377
0.009982964140407377

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.009982964140407377 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC #97.7%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------

0.010688089331915769

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.010688089331915769 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC #98%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------

0.006497616042128936

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.008028770160870459 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC #96.5%
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC get_clf_eval(final_answer['label'], final_answer['outlier'])

# COMMAND ----------

final_answe

# COMMAND ----------

# MAGIC %python
# MAGIC ### 97%
# MAGIC from sklearn.metrics import classification_report
# MAGIC final_answer = df_answer_diff.toPandas()
# MAGIC print(classification_report(final_answer['label'], final_answer['outlier']))

# COMMAND ----------

# MAGIC %python
# MAGIC final_spark.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC #final_spark = spark.createDataFrame(final_answer)
# MAGIC #final_spark = spark.createDataFrame(df_answer_diff)
# MAGIC final_spark.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/pre_test_dffits.csv")

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when a.dffits >= 0.0086705960774002 then 1 else 0 end outlier, b.label 
# MAGIC from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.registerTempTable("df_answer_diff")

# COMMAND ----------

final <- SparkR::sql("select * from df_answer_diff")

# COMMAND ----------

head(final)

# COMMAND ----------

fianl_spark <- createDataFrame(final)

# COMMAND ----------

pred <- prediction(as.numeric(final$outlier), as.numeric(final$label))

# COMMAND ----------

final

# COMMAND ----------

library(ROCR)
ROCRpred <- prediction(as.numeric(final$outlier), final$label)
ROCRperf <- performance(ROCRpred, 'tpr', 'fpr')
plot(ROCRperf, colorize = TRUE, text.adj = c(-0.2, 1.7))

# COMMAND ----------

typeof(as.list(final$label))

# COMMAND ----------

library(caret)
confusionMatrix(data = factor(final$label), reference = factor(final$outlier), mode = "prec_recall")

# COMMAND ----------

final_answer['label'], final_answer['outlier']

# COMMAND ----------

# MAGIC %sql 
# MAGIC select a.id, a.dffit_outlier, b.label from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id

# COMMAND ----------

# MAGIC %python
# MAGIC from yellowbrick.classifier import ROCAUC

# COMMAND ----------

# MAGIC %python
# MAGIC visualizer.score(final_answer['outlier'], final_answer['label'])
# MAGIC visualizer.show()

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import roc_curve
# MAGIC fper, tper, thresholds = roc_curve(final_answer['label'], prob)
# MAGIC plot_roc_curve(fper, tper)

# COMMAND ----------

final_answer['label'], final_answer['outlier']

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
# MAGIC roc_curve_plot(final_answer['label'], final_answer['outlier'])

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC roc_curve_plot(final_answer['outlier'],final_answer['label'])

# COMMAND ----------

# MAGIC %python
# MAGIC result = spark.sql("select a.id, a.dffit_outlier, b.label from df_answer as a left join sparkDF as b on 1=1 and a.id = b.id")
# MAGIC result.registerTempTable("result")
# MAGIC final = result.toPandas()
# MAGIC final

# COMMAND ----------

# MAGIC %python
# MAGIC #final = final.toPandas()
# MAGIC final['dffit_outlier'] = final['dffit_outlier'].astype(int)

# COMMAND ----------

# MAGIC %python
# MAGIC final

# COMMAND ----------

library(tidyverse)
kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()