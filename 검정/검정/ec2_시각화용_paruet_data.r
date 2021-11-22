# Databricks notebook source
library(SparkR)
kart_df <- read.df( "/FileStore/KartRider/RegResult/outlierdetection_regfinal.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

createOrReplaceTempView(kart_df, "kart_df")

# COMMAND ----------

# MAGIC %python 
# MAGIC first = spark.sql("select * from kart_df")

# COMMAND ----------

# MAGIC %python 
# MAGIC first.count()

# COMMAND ----------

# MAGIC %python 
# MAGIC first.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/shiny_data/main_data.parquet")

# COMMAND ----------

library(SparkR)
result <- read.df( "/FileStore/KartRider/RegResult/rstudent_outlierprediction.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# COMMAND ----------

createOrReplaceTempView(result, "result")

# COMMAND ----------

# MAGIC %python 
# MAGIC second = spark.sql("select * from result")

# COMMAND ----------

# MAGIC %python 
# MAGIC second.count()

# COMMAND ----------

# MAGIC %python 
# MAGIC second.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/shiny_data/sub_data.parquet")

# COMMAND ----------

# MAGIC %python 
# MAGIC august = (spark
# MAGIC   .read                                              
# MAGIC   .option("inferSchema","true")                 
# MAGIC   .option("header","true")                           
# MAGIC   .parquet("/FileStore/KartRider/shiny_data/main_data.parquet")) 

# COMMAND ----------

# MAGIC %python
# MAGIC august.display()

# COMMAND ----------

# MAGIC %python 
# MAGIC august.write.format("parquet").mode("overwrite").option("header","true").save("/FileStore/KartRider/ec2_eda/rstudentdetection.parquet")