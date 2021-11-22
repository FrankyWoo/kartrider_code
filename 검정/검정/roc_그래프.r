# Databricks notebook source
library(SparkR)
df <- read.df("/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

head(df)

# COMMAND ----------

library(tidyverse)
df <- df %>% as.data.frame() %>% as_tibble()

# COMMAND ----------

df

# COMMAND ----------

library(ROCR)
ROCRpred <- prediction(df$outlier, df$label)
ROCRperf <- performance(ROCRpred, 'tpr', 'fpr')
plot(ROCRperf, colorize = TRUE, text.adj = c(-0.2, 1.7))