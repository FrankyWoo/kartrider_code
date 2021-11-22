# Databricks notebook source
library(SparkR)
kart_df <- read.df( "/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

nrow(kart_df)

# COMMAND ----------

# MAGIC %sh
# MAGIC zip my_compressed.zip /dbfs/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC cp -r /dbfs/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv ./my_temp_folder

# COMMAND ----------

# MAGIC %sh
# MAGIC zip -r my_compressed3.zip my_temp_folder

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -al

# COMMAND ----------

# MAGIC %sh
# MAGIC mv my_compressed3.zip /dbfs/FileStore/KartRider/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/FileStore/KartRider/

# COMMAND ----------

data <- read.df("/FileStore/KartRider/RegResult/versionA_label.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")
data

# COMMAND ----------

library(tidyverse)
kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()

# COMMAND ----------

kart_tibble_pyspark <- createDataFrame(kart_tibble)
createOrReplaceTempView(data, "data")
createOrReplaceTempView(kart_tibble_pyspark, "kart_tibble")

# COMMAND ----------

df_answer <- SparkR::sql("select * from kart_tibble where track in ('브로디 비밀의 연구소','차이나 골목길 대질주','차이나 용의 운하','빌리지 운명의 다리','대저택 은밀한 지하실')")
createOrReplaceTempView(df_answer, "df_answer")

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select a.id, case when Abs(a.rstudent) > 1.0809736803124876 then 1 else 0 end outlier, b.label as label
# MAGIC from df_answer as a left join data as b on 1=1 and a.id = b.id
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix,roc_auc_score
# MAGIC 
# MAGIC def get_clf_eval(y_test, pred):
# MAGIC     confusion = confusion_matrix(y_test, pred, average = "macro")
# MAGIC     accuracy = accuracy_score(y_test, pred, average = "macro")
# MAGIC     precision = precision_score(y_test, pred, average = "macro")
# MAGIC     recall = recall_score(y_test, pred, average = "macro")
# MAGIC     roc_auc = roc_auc_score(y_test, pred, average = "macro")
# MAGIC     print('Confusion Matrix')
# MAGIC     print(confusion)
# MAGIC     print('정확도:{}, 정밀도:{}, 재현율:{}, roc_auc:{}'.format(accuracy, precision, recall, roc_auc))
# MAGIC     

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
# MAGIC #     print(f1_score(temp["label"].astype(str), temp["outcome"].astype(str), average="macro"))
# MAGIC # print(precision_score(temp["label"].astype(str), temp["outcome"].astype(str), average="macro"))
# MAGIC # print(recall_score(temp["label"].astype(str), temp["outcome"].astype(str), average="macro")) 

# COMMAND ----------

# MAGIC %python
# MAGIC #0.94
# MAGIC panda = df_answer_diff.toPandas()
# MAGIC print(panda['outlier'].value_counts())
# MAGIC print(panda['label'].value_counts())
# MAGIC get_clf_eval(panda['label'], panda['outlier'])

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.registerTempTable("df_answer_diff")

# COMMAND ----------


df <- SparkR::sql("select * from df_answer_diff")

# COMMAND ----------

df2<- df %>% as.data.frame() %>% as_tibble()

# COMMAND ----------

df2

# COMMAND ----------

library(ROCR)
ROCRpred <- prediction(df2$outlier, df2$label)
ROCRperf <- performance(ROCRpred, 'tpr', 'fpr')
plot(ROCRperf, colorize = TRUE, text.adj = c(-0.2, 1.7))