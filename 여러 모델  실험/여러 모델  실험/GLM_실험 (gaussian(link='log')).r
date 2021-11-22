# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA_id.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

quasi= glm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, family=quasi, data=kart_tibble)
#summary(gaussian)

# COMMAND ----------

summary(quasi)

# COMMAND ----------

BIC(quasi)

# COMMAND ----------

library(tidyverse)
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

#### 가우시안 
gaussian= glm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, family=gaussian, data=kart_tibble)
summary(gaussian)

# COMMAND ----------

AIC(gaussian)

# COMMAND ----------

BIC(gaussian)

# COMMAND ----------

1 - gaussian$deviance/gaussian$null.deviance

# COMMAND ----------

save(gaussian, file="/dbfs/FileStore/model/gaussian_glm.RData")  

# COMMAND ----------

# MAGIC %md 
# MAGIC log log 

# COMMAND ----------

log_glm = glm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, family=gaussian(link="log"), data=kart_tibble)
summary(log_glm)

# COMMAND ----------

print(rsq(log_glm))
print(rsq(log_glm,adj=TRUE))

# COMMAND ----------

print(AIC(log_glm))
print(BIC(log_glm))

# COMMAND ----------

save(log_glm, file="/dbfs/FileStore/model/log_glm.RData")  

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

n <- nrow(kart_tibble)
n

# COMMAND ----------

k <- length(kart_lm$coefficients)-1
R2 <- 0.6934396

# COMMAND ----------

k <- length(gaussian$coefficients)-1
R2 <- 0.6934396
#1 – [(1-R2)*(n-1)/(n-k-1)]

# COMMAND ----------

(1-R2)*(n-1)

# COMMAND ----------

(n-k-1)

# COMMAND ----------

383693.4/1251308