# Databricks notebook source
library(SparkR)
kart_df <- read.df("/FileStore/KartRider/versionA.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

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

# MAGIC %md 
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Cook's distance 

# COMMAND ----------

#find Cook's distance for each observation in the dataset
cooksD <- cooks.distance(kart_lm)

# Plot Cook's Distance with a horizontal line at 4/n to see which observations
#exceed this thresdhold
n <- nrow(kart_tibble)
plot(cooksD, main = "Cooks Distance for Influential Obs")
abline(h = 4/n, lty = 2, col = "steelblue") # add cutoff line

# COMMAND ----------

# MAGIC %md
# MAGIC cook's distance 이용한 outlier 비율 계산

# COMMAND ----------

#identify influential points
influential_obs <- as.numeric(names(cooksD)[(cooksD > (4/n))])

#define new data frame with influential points removed
outliers_removed <- kart_tibble[-influential_obs, ]

# COMMAND ----------

52605/1251609 ###### 기준을 4/n으로 하였을 때 비율.... 

# COMMAND ----------

mean(cooksD)

# COMMAND ----------

cutoffbasedonmean <-mean(cooksD) * 3

# COMMAND ----------

cooksD > cutoffbasedonmean

# COMMAND ----------

length(names(cooksD)[(cooksD > cutoffbasedonmean)])

# COMMAND ----------

44744/1251609  ### mean * 3 기준

# COMMAND ----------

cooksD > cutoffbasedonmean

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

cooksD > (4/n)

# COMMAND ----------

names(cooksD)

# COMMAND ----------

names(cooksD)[(cooksD > (4/n))]

# COMMAND ----------

length(names(cooksD)[(cooksD > (4/n))])

# COMMAND ----------

4/1964

# COMMAND ----------

4/n

# COMMAND ----------

cooksD

# COMMAND ----------

length(influential_obs)

# COMMAND ----------

influential_obs

# COMMAND ----------

typeof(influential_obs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 그래프

# COMMAND ----------

library(ggplot2)
library(gridExtra)

# COMMAND ----------

library(olsrr)

# COMMAND ----------

library(rlang)

# COMMAND ----------

par(mfrow=c(2,2))
plot(kart_lm)

# COMMAND ----------

### MULTICOLLINEARITY
library(car)
vif(kart_lm)

# COMMAND ----------

r <- rstudent(kart_lm) # create an named numerical object

# COMMAND ----------

# MAGIC %md
# MAGIC ## IDENTIFYING OUTLIERS

# COMMAND ----------

p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble

# COMMAND ----------

outliers <- kart_tibble[,c("p_matchTime","channelName","p_rankinggrade2","teamPlayers","track","gameSpeed")]

# COMMAND ----------

### 하나씩

# COMMAND ----------

outliers$r <- rstudent(kart_lm)

# COMMAND ----------

r <- rstudent(kart_lm)

# COMMAND ----------

plot(outliers$r, 
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

plot(outliers$r, 
     xlab="Index",
     ylab="studentized residuals",
     pch=19)


# COMMAND ----------

plot(outliers$r, 
     xlab="Index",
     ylab="studentized residuals",
     pch=19)
abline(h = 3, col="red")  # add cutoff line
abline(h = -3, col="red")  # add cutoff line

# COMMAND ----------

rstudent1<-subset(outliers,abs(r)>3)

# COMMAND ----------

rstudent1

# COMMAND ----------

View(rstudent1)

# COMMAND ----------

outliers$lev <- hatvalues(kart_lm)

# COMMAND ----------

outliers

# COMMAND ----------

typeof(outliers)

# COMMAND ----------

outliers <- tibble::rowid_to_column(outliers, "ID")

# COMMAND ----------

outliers

# COMMAND ----------

plot(outliers$lev, 
     xlab="Index",
     ylab="leverage",
     pch=19)
abline(lm(lev~ID, data=outliers), col="red")  # Add a fitted line 

# COMMAND ----------

plot(kart_lm, which = 5)### 왜 which = 5??

# COMMAND ----------

leverage1<-subset(outliers,lev > .03)
# Use the view function to examine observations with leverage values greater than .03
view(leverage1)

# COMMAND ----------

leverage1

# COMMAND ----------

length(leverage1)

# COMMAND ----------

nrow(leverage1)

# COMMAND ----------

outliers$lev > .03

# COMMAND ----------

length(which(outliers$lev > .03))

# COMMAND ----------

outliers

# COMMAND ----------

outliers$cd <- cooks.distance(kart_lm)

# COMMAND ----------

nrow(kart_tibble)

# COMMAND ----------

m_cd <- mean(outliers$cd)
m_cd

# COMMAND ----------

cd_cutoff

# COMMAND ----------

cd_cutoff <- m_cd * 3

# COMMAND ----------

plot(cooks.distance(kart_lm), pch="*", cex=2, main="Influential Obs by Cook's distance Using Official Threshold (a Cook’s D of more than 3 times the mean) ")  
abline(h = cd_cutoff, col="red")  # add cutoff line

# COMMAND ----------

# MAGIC %md
# MAGIC threshold : n/4

# COMMAND ----------

tt <- 4/1251609
tt

# COMMAND ----------

length(which(outliers$cd > tt))

# COMMAND ----------

 52605/1251609

# COMMAND ----------

plot(cooks.distance(kart_lm), pch="*", cex=2, main="Influential Obs by Cook's distance Using Official Threshold (4/n)")  
abline(h = 4/1251609, col="red")  # add cutoff line

# COMMAND ----------

library(olsrr)
ols_plot_cooksd_bar(kart_lm) 

# COMMAND ----------

ols_plot_cooksd_bar(kart_lm) 

# COMMAND ----------

ols_plot_cooksd_chart(kart_lm)

# COMMAND ----------

large_cd<-subset(outliers, cd > (4/1251609)) 
View(large_cd)

# COMMAND ----------

hist(large_cd$cd)

# COMMAND ----------

quantile(large_cd$cd, probs = seq(0, 1, 0.05))  # from 0 to 1, by 0.05 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

outliers$cd <- cooks.distance(kart_lm)

# COMMAND ----------

outliers$dffit<- dffits(kart_lm)

# COMMAND ----------

dfbetaR<-dfbetas(kart_lm)

# COMMAND ----------

dfbetaR <- as.data.frame(dfbetaR)

# COMMAND ----------

dfbetas(kart_lm)

# COMMAND ----------

outliers$cd <- cooks.distance(kart_lm)
outliers$dffit<- dffits(kart_lm)
# In this step we are creating a separate data frame with the dfbetas so that we can properly label the columns
dfbetaR<-dfbetas(kart_lm)
dfbetaR <- as.data.frame(dfbetaR)
dfbetas(kart_lm)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


plot(outliers$r, 
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

kart_lm

# COMMAND ----------

ols_plot_cooksd_chart(kart_lm)

# COMMAND ----------

#create scatterplot with outliers present
outliers_present <- ggplot(data = kart_tibble, aes(x = x, y = y)) +
  geom_point() +
  geom_smooth(method = lm) +
  ylim(0, 200) +
  ggtitle("Outliers Present")

#create scatterplot with outliers removed
outliers_removed <- ggplot(data = outliers_removed, aes(x = x, y = y)) +
  geom_point() +
  geom_smooth(method = lm) +
  ylim(0, 200) +
  ggtitle("Outliers Removed")

#plot both scatterplots side by side
gridExtra::grid.arrange(outliers_present, outliers_removed, ncol = 2)

# COMMAND ----------

plot(cooks.distance(lm3), pch="*", cex=2, main="Influential Obs by Cook's distance Using Official Threshold (4/1964)")  
abline(h = 4/1964, col="red")  # add cutoff line

# COMMAND ----------

colnames(kart_tibble)

# COMMAND ----------

influential_obs <- names(cooksD)[(cooksD >= (4/n))] # Row names
influential_obs <- which(cooksD >= 4/n)             # Row numbers

# COMMAND ----------

cooksd <- cooks.distance(kart_lm)

# Plot the Cook's Distance using the traditional 4/n criterion
sample_size <- nrow(kart_tibble)
plot(cooksd, pch="*", cex=2, main="Influential Obs by Cooks distance")  # plot cook's distance
abline(h = 4/sample_size, col="red")  # add cutoff line
text(x=1:length(cooksd)+1, y=cooksd, labels=ifelse(cooksd>4/sample_size, names(cooksd),""), col="red")  # add labels

# COMMAND ----------

cooksd

# COMMAND ----------

typeof(cooksd)

# COMMAND ----------

cooks <- as.data.frame(cooks.distance(kart_lm))

# COMMAND ----------

cooks

# COMMAND ----------

df <- as.DataFrame(cooks)
head(df)

# COMMAND ----------

names(df)[names(df) == "cooks_distance(kart_lm)"] <- "cooks_distance"

# COMMAND ----------

head(df, 10)

# COMMAND ----------

write.df(df, path ="/FileStore/KartRider/cooksdistance2.csv", source = "csv", mode = "overwrite")

# COMMAND ----------

ddd <- read.df("/FileStore/KartRider/cooksdistance2.csv", "csv", header = "false", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

head(ddd)

# COMMAND ----------

/dbfs/FileStore/KartRider/cooksdistance.csv/part-00000-7be2d52c-276e-470f-9253-c1a26c3baa43-c000.snappy.parquet

# COMMAND ----------

### cook's distance dataframe으로!
df <- as.DataFrame(cooksd)

# COMMAND ----------

df3 <- createDataFrame(cooksd)

# COMMAND ----------

plot(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data = kart_tibble)
points(p_matchTime ~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data = kart_tibble[influential_obs,], col="red", pch=19)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

panel.cor <- function(x, y, digits=2, prefix="", cex.cor, ...)
+ {
+   usr <- par("usr"); on.exit(par(usr)) #on.exit() par()함수 인자가 있으면 실해
+   par(usr = c(0, 1, 0, 1))
+   r <- abs(cor(x, y)) #상관계수 절대값
+   txt <- format(c(r, 0.123456789), digits=digits)[1] #상관계수 자릿수 지정
+   txt <- paste(prefix, txt, sep="")
+   if(missing(cex.cor)) cex.cor <- 0.8/strwidth(txt)
+   text(0.5, 0.5, txt, cex = cex.cor * r) #상관계수 크기에 비례하게 글자지정
+ }


# COMMAND ----------

pairs(~ channelName+p_rankinggrade2+teamPlayers+track+gameSpeed, data=kart_tibble,
+     lower.panel=panel.smooth,pch=20, main="Kartrider Scatterplot Matrix")

# COMMAND ----------

r <- rstudent(kart_lm)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dfbetas

# COMMAND ----------

dfbetaR<-dfbetas(kart_lm) ### 도저히 안됩니다ㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠ

# COMMAND ----------

dfbetaR <- as.data.frame(dfbetaR)
#dfbetas(lm3)

# COMMAND ----------

