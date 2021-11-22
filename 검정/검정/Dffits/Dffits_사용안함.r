# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

kart_df

# COMMAND ----------

#### 저장한 파일 확인
#ddd <- read.df("/FileStore/KartRider/dffits4.csv", "csv", header = "false", inferSchema = "true", na.strings = "NA")

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

dffits(kart_lm)

# COMMAND ----------

dffits <- as.data.frame(dffits(kart_lm))

# COMMAND ----------

### dffits를 dataframe으로!
df <- as.DataFrame(dffits)

# COMMAND ----------

### dffits 계산 확인 
dffits

# COMMAND ----------

### 모든 데이터 변환 잘 되었는 지 확인. 
tail(dffits)

# COMMAND ----------

#### 10000 번째 데이터 확인 
dffits['dffits(kart_lm)'][10000,]

# COMMAND ----------

### dffits type 확인
typeof(dffits)

# COMMAND ----------

# library(dplyr)
# dataFrame <- dffits %>% 
#   as_tibble()
# dataFrame

# COMMAND ----------

names(df)[names(df) == "dffits(kart_lm)"] <- "dffits"

# COMMAND ----------

### 저장 write.df(df, path ="/FileStore/KartRider/dffits.csv", mode = "overwrite")

# COMMAND ----------

### 이 방법 최종 저장 write.df(df, "/FileStore/KartRider/dffits5.csv", source="csv" ,  header = "true")

# COMMAND ----------

library(olsrr)
ols_plot_dffits(kart_lm)

# COMMAND ----------

nrow(kart_df)

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

#find number of predictors in model
p <- length(kart_lm$coefficients)-1

#find number of observations
n <- nrow(kart_df)

#calculate DFFITS threshold value
thresh <- 2*sqrt(p/n)

thresh

# COMMAND ----------

#find number of predictors in model
p <- length(kart_lm$coefficients)-1

#find number of observations
n <- nrow(kart_df)

pp <- p+1
#calculate DFFITS threshold value
thresh2 <- 2*sqrt(pp/n)

thresh2

# COMMAND ----------

#plot DFFITS values for each observation
plot(dffits(kart_lm), type = 'h')

#add horizontal lines at absolute values for threshold
abline(h = thresh, lty = 2)
abline(h = -thresh, lty = 2)

# COMMAND ----------

#plot DFFITS values for each observation
plot(dffits(kart_lm), type = 'h')

#add horizontal lines at absolute values for threshold
abline(h = thresh2, lty = 2)
abline(h = -thresh2, lty = 2)

# COMMAND ----------

##thresh가 2일 때
#plot DFFITS values for each observation
plot(dffits(kart_lm), type = 'h')

#add horizontal lines at absolute values for threshold
abline(h = 2, lty = 2)
abline(h = -2, lty = 2)

# COMMAND ----------

#sort observations by DFFITS, descending
dffits[order(-dffits['dffits(kart_lm)']), ]

# COMMAND ----------

### thresh보다 큰 값
th_1 <- dffits > thresh
th_1

# COMMAND ----------

length(which(dffits >=  2))

# COMMAND ----------

length(which(dffits >  0.03096394))

# COMMAND ----------

length(which(dffits >  0.03101551))

# COMMAND ----------

length(which(dffits >=  0.03096394))

# COMMAND ----------

length(which(dffits >=  0.03101551))

# COMMAND ----------

nrow(df)

# COMMAND ----------

nrow(df[df$dffits >= 0.03096394,]) ###0.02354409

# COMMAND ----------

nrow(df[df$dffits >= 0.03101551,]) ###0.02345381

# COMMAND ----------

# MAGIC %md
# MAGIC 0.03101551 기준으로 dffits 값 outlier 탐지 

# COMMAND ----------

head(df)

# COMMAND ----------

# add column to dataframe
df$outlier <- df$dffits >=  0.03101551

# COMMAND ----------

head(df)

# COMMAND ----------

head(df, 100)

# COMMAND ----------

createOrReplaceTempView(df, "df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM df WHERE outlier = True

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("SELECT * FROM df").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("SELECT * FROM df WHERE outlier = True").show()

# COMMAND ----------

# MAGIC %md
# MAGIC memory 사용 이슈로 이용하지 못한 코드. (rstandard, dfbetas 관련)

# COMMAND ----------

rstandard

# COMMAND ----------

library(tidytable)

# COMMAND ----------

kart_xdata <- kart_tibble %>%
  select(channelName, p_rankinggrade2, teamPlayers, track, gameSpeed)

# COMMAND ----------

kart_xdata_dummy <- fastDummies::dummy_cols(kart_xdata) 

# COMMAND ----------

X = as.matrix(kart_xdata_dummy[-1:-5])
Y = as.matrix(kart_tibble %>% select(p_matchTime))

# COMMAND ----------

beta <- solve(t(X)%*%X) %*% t(X) %*%Y

# COMMAND ----------

beta <- solve(t(X)%*%X) %*% t(X) %*%Y
beta <- round(beta, digits=5)

# COMMAND ----------

beta <- solve(t(X)%*%X) %*% t(X) %*%Y
yhat <-  X %*%  beta
H <- X %*% solve(t(X)%*%X)%*%t(X)

Hdiag <- diag(H); resid <- Y - yhat
n <- norw(Y)

DFFITS <- rep(0,n); CookD <- rep(0,n); Maha <- rep(0,n)
COVRATIO <- rep(0,n)

# COMMAND ----------

for (i in 1:n){
  Xi <- X[-i, ] ; Yi <- Y[-i]
  xbar <- apply(X, 2, mean)
  betai <- solve(t(Xi) %*% Xi) %*% t(Xi) %*% Yi
  yhati <- Xi %*% betai
  erri <- Yi - yhati
  si <- sqrt( (t(erri)%*%erri)*(1/(n-1-ncol(X))) )
  DFFITS[i] <- (t(X[i,]) %*% (beta-betai)) * (1/(si*sqrt(Hdiag[i])) )
  CookD[i] <- ((Hdiag[i] * resid[i]^2)) / (sigma^2 * (1-Hdiag[i]^2)) * (1/ncol(X))
  Maha[i] <- t(X[i,] -  xbar) %*% solve(t(X)%*%X) %*% (X[i,] - xbar) * (n-1)
  cov1 <- det(solve(t(Xi) %*% Xi) * rep(si^2, 4))
  cov2 <- det(solve(t(X) %*% X) * rep(sigma^2, 4))
  COVRATIO[i] <- cov1/cov2
}
  
  
DFFITS <- round(DFFITS, digit=5)
CookD <- round(CookD, digit=5)
Maha <- round(Maha, digit=5)
COVRATIO <- round(COVRATIO, digit=5)

list(DFFITS=DFFITS, CookD=CookD, Mahalanobis = Maha, COVRATIO)

# COMMAND ----------

influence.measures( kart_lm )

# COMMAND ----------

influence.measures( kart_lm )$is.inf

# COMMAND ----------

standard_res <- rstandard(kart_lm)

# COMMAND ----------

dfbetas <- as.data.frame(dfbetas(kart_lm))