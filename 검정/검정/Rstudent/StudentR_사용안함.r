# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

library(tidyverse)

# COMMAND ----------

nrow(kart_tibble)

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

kart_lm

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

c <- covratio(kart_lm)

# COMMAND ----------

c

# COMMAND ----------



# COMMAND ----------

r <- rstudent(kart_lm)

# COMMAND ----------

r1 <- as.data.frame(rstudent(kart_lm))

# COMMAND ----------

df <- as.DataFrame(r1)

# COMMAND ----------

write.df(df, path ="/FileStore/KartRider/rstudent.csv", source = "csv", mode = "overwrite")

# COMMAND ----------

ddd <- read.df("/FileStore/KartRider/rstudent.csv", "csv", header = "false", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

head(ddd)

# COMMAND ----------

r

# COMMAND ----------

length(which(r > 3))

# COMMAND ----------

length(which(r < -3))

# COMMAND ----------

17034 + 4466

# COMMAND ----------

21500/1251609

# COMMAND ----------

length(which(r > 2))

# COMMAND ----------

length(which(r < -2))

# COMMAND ----------

38944 + 11970

# COMMAND ----------

50914/1251609

# COMMAND ----------

length(which(r > 2.5))

# COMMAND ----------

length(which(r < -2.5))

# COMMAND ----------

25674 + 7339

# COMMAND ----------

33013/1251609

# COMMAND ----------

length(which(r > 3) & which(r < -3))

# COMMAND ----------

length(which(r > 3)&which(r < -3))

# COMMAND ----------

17034/1251609

# COMMAND ----------

which(r > 3)&which(r < -3)

# COMMAND ----------

which(r < -3)

# COMMAND ----------

r[5465]

# COMMAND ----------

length(which(r > 2.5)&which(r < -2.5))

# COMMAND ----------

4696/1251609

# COMMAND ----------

which(r > 2.5)&which(r < -2.5)

# COMMAND ----------

length(which(r > 2)&which(r < -2))

# COMMAND ----------

38944/1251609

# COMMAND ----------

which(r > 2)&which(r < -2)

# COMMAND ----------

plot(r, 
     xlab="Index",
     ylab="studentized residuals",
     pch=19)
abline(h = 2, col="red")  # add cutoff line
abline(h = -2, col="red")  # add cutoff line

# COMMAND ----------

plot(r, 
     xlab="Index",
     ylab="studentized residuals",
     pch=19)
abline(h = 3, col="red")  # add cutoff line
abline(h = -3, col="red")  # add cutoff line
text(x=1:length(r), y=r, labels=ifelse(r > 3,names(r),""), col="red", pos = 4)  # add labels # pos = 4 to position at the right 
text(x=1:length(r), y=r, labels=ifelse(r<(-3.0),names(r),""), col="red", pos = 4)  # add labels # pos = 4 to position at the right 

# Or, use this code for interactive point identification to avoid overlapping labels
identify(1:length(r),r,row.names(outliers))

# COMMAND ----------

outliers <- kart_tibble[,c("p_matchTime","channelName","p_rankinggrade2","teamPlayers","track","gameSpeed")]

# COMMAND ----------

library(olsrr)
ols_plot_resid_stand(kart_lm)

# COMMAND ----------

library(car)

outlierTest(kart_lm)

# COMMAND ----------

c <- covratio(kart_lm)

# COMMAND ----------

length(which(c > 1.000721))

# COMMAND ----------

length(which(c < 0.9992785))

# COMMAND ----------

73265 + 43744

# COMMAND ----------

length(which(c > 1.000719))

# COMMAND ----------

length(which(c < 0.9992809))

# COMMAND ----------

43803 + 73420

# COMMAND ----------

nrow()

# COMMAND ----------

117009/1251609

# COMMAND ----------

117223/1251609

# COMMAND ----------

