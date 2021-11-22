# Databricks notebook source
library(SparkR)
kart_df <- read.df( "/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

head(kart_df)

# COMMAND ----------

library(tidyverse)
kart_tibble <- kart_df %>% as.data.frame() %>% as_tibble()
head(kart_tibble)

# COMMAND ----------

smoothScatter(kart_tibble$id, kart_tibble$rstudent)

# COMMAND ----------

### Rstudent
smoothScatter(kart_tibble$rstudent , 
     xlab="Index",
     ylab="studentized residuals",
     pch=19)
abline(h = 1.0809736803124876, col="red")  # add cutoff line
abline(h = -1.0809736803124876, col="red")  # add cutoff line


# COMMAND ----------

### Cook's distance
smoothScatter(kart_tibble$cooks_distance , 
     xlab="Index",
     ylab="Cook's Distance",
     pch=19)
abline(h = 3.861943275329788e-7, col="red")  # add cutoff line
#abline(h = -1.0809736803124876, col="red")  # add cutoff line


# COMMAND ----------

### Dffits
smoothScatter(kart_tibble$dffits , 
     xlab="Index",
     ylab="dffits",
     pch=19)
abline(h = 0.011141143149623079, col="red")  # add cutoff line
abline(h = -0.011141143149623079, col="red")  # add cutoff line


# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA 

# COMMAND ----------

### 
# Histogram with kernel density curve
library(ggplot2)
ggplot(kart_tibble, aes(x = kart_tibble$p_matchTime)) +
    geom_density(alpha = .2, fill = "#FF6666") +labs(x = "matchtime")
	

# COMMAND ----------

### channelName+p_rankinggrade2+teamPlayers+track+gameSpeed kart_tibble

# COMMAND ----------

lapply(names(kart_tibble$channelName),
    function(x) 
	ggplot(kart_tibble$channelName, aes(get(x))) +
		geom_bar() +
		theme(axis.text.x = element_text(angle = 90)))
theme(axis.text.x = element_text(angle = 90))

# COMMAND ----------

ggplot(data = kart_tibble, aes(x = channelName)) +
    geom_bar(fill = "#81c147") + theme(axis.text.x = element_text(size = 12, angle =90))


# COMMAND ----------

ggplot(data = kart_tibble, aes(x = p_rankinggrade2)) +
    geom_bar(fill = "#81c147") + theme(axis.text.x = element_text(size = 12, angle =90))#FF6666


# COMMAND ----------

ggplot(data = kart_tibble, aes(x = teamPlayers)) +
    geom_bar(fill = "#81c147") + theme(axis.text.x = element_text(size = 12, angle =90))

# COMMAND ----------

ggplot(data = kart_tibble, aes(x = gameSpeed)) +
    geom_bar(fill = "#81c147") + theme(axis.text.x = element_text(size = 12, angle =90))

# COMMAND ----------

createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff = spark.sql("""
# MAGIC select id, case when Abs(rstudent) > 1.0809736803124876 then 1 else 0 end outlier 
# MAGIC from kart_df 
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.display()

# COMMAND ----------

# MAGIC %python
# MAGIC df_answer_diff.registerTempTable('df_answer_diff')

# COMMAND ----------

library(ggplot2)
test <- SparkR::sql("select * from df_answer_diff")

# COMMAND ----------

head(test)

# COMMAND ----------

library(tidyverse)
kart_tibble2 <- test %>% as.data.frame() %>% as_tibble()

# COMMAND ----------

ggplot(data = kart_tibble2, aes(x = outlier)) +
    geom_bar(fill = "#81c147") + theme(axis.text.x = element_text(size = 12, angle =90))

# COMMAND ----------

# ### df_answer = spark.sql(""" 
# select a.id as id, case when a.cooks_distance > 0.00002491911 then 1 else 0 end cooks_outlier, b.label as label
# from kart_tibble2 as a left join sparkDF as b on 1=1 and a.id = b.id
# """)

# COMMAND ----------

df4 = spark.sql("""
select a.*, b.name
from df_tmp as a
left join kart_meta as b
on 1=1
and a.p_kart = b.id
""")


# COMMAND ----------

# MAGIC %python
# MAGIC ### 임시조인
# MAGIC finalanswer = spark.sql(""" 
# MAGIC select a.*, b.outlier from kart_df as a left join df_answer_diff as b on 1=1 and a.id = b.id""")

# COMMAND ----------

# MAGIC %python
# MAGIC finalanswer.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC finalanswer.registerTempTable('finalanswer')

# COMMAND ----------

# MAGIC %python 
# MAGIC finalanswer.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC finalanswer.count()

# COMMAND ----------

# MAGIC %python 
# MAGIC finalanswer.write.format("csv").mode("overwrite").option("header","true").save("/FileStore/KartRider/RegResult/rstudent_outlierprediction.csv")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select outlier, count(*) from finalanswer group by outlier

# COMMAND ----------

outlierdetection <- SparkR::sql("select * from finalanswer")

# COMMAND ----------

ggplot(data = outlierdetection, aes(x = gameSpeed)) +
    geom_bar(fill = "#81c147") + theme(axis.text.x = element_text(size = 12, angle =90))

# COMMAND ----------

detection <- outlierdetection %>% as.data.frame() %>% as_tibble()

# COMMAND ----------

detection <- detection %>%
  mutate(
         outlier = as.factor(outlier))

# COMMAND ----------

ggplot(detection, aes(x = gameSpeed, fill = outlier)) +
    geom_bar(position = "fill") +
    theme_classic() 

# COMMAND ----------

ggplot(detection, aes(x = teamPlayers, fill = outlier)) +
    geom_bar(position = "fill") +
    theme_classic() 


# COMMAND ----------

ggplot(detection, aes(x = p_rankinggrade2, fill = outlier)) +
    geom_bar(position = "fill") +
    theme_classic() 


# COMMAND ----------

ggplot(detection, aes(x = channelName, fill = outlier)) +
    geom_bar(position = "fill") +
    theme_classic() + theme(axis.text.x = element_text(size = 12, angle = 90)) 