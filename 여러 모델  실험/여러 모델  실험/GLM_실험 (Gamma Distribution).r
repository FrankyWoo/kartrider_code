# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA_id.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

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

# kart_lm <- lm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble)

# COMMAND ----------

# summary(kart_lm)

# COMMAND ----------

kart_tibble

# COMMAND ----------

### Min-Max scaling
kart_tibble$scale_y <- scales::rescale(kart_tibble$p_matchTime, to=c(0,1))

# COMMAND ----------

kart_tibble

# COMMAND ----------

kart_tibble$scale_y

# COMMAND ----------

#### 전처리 

# COMMAND ----------

### 우선 log-log
gamma_glm = glm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, family=Gamma, data=kart_tibble)

# COMMAND ----------

summary(gamma.glm)

# COMMAND ----------

library(rsq)
rsq(gamma.glm,adj=TRUE) #Adjusted R-squared Value

# COMMAND ----------

rsq(gamma.glm) #R-sqaured Value

# COMMAND ----------

AIC(gamma.glm)

# COMMAND ----------

BIC(gamma.glm)

# COMMAND ----------

save(gamma.glm, file="/dbfs/FileStore/model/gamma_glm.RData")  

# COMMAND ----------

### File 저장 잘 되었는 지 확인 
load(file='/dbfs/FileStore/model/gamma_glm.RData')

# COMMAND ----------

gamma.glm

# COMMAND ----------

ls()

# COMMAND ----------

summary(log.glm)

# COMMAND ----------

head(summary(test))

# COMMAND ----------

summary(test)

# COMMAND ----------

library(pscl) ### 로그 트랜스포메이션 +  family=gaussian(link="log")
pR2(log.glm)

# COMMAND ----------

# MAGIC %md
# MAGIC Log-log Gaussian(link=log) Distribution 

# COMMAND ----------

###kart_tibble 파일 확인
kart_tibble

# COMMAND ----------

# MAGIC %md 
# MAGIC Gaussian Distribution

# COMMAND ----------

gaussian.glm = glm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, family=gaussian, data=kart_tibble)
summary(gaussian.glm)

# COMMAND ----------

library(pscl)
pR2(gaussian.glm)

# COMMAND ----------

kart_tibble$scale_y

# COMMAND ----------

for (val in kart_tibble$scale_y){
  if(val<0 | val>1)  print(val)
}

# COMMAND ----------

gaussiannnnnn = glm(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, family=gaussian, data=kart_tibble2)
summary(gaussiannnnnn)