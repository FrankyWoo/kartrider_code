# Databricks notebook source
#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/final3df.csv"))

final.registerTempTable("final_df")


# COMMAND ----------

village = spark.sql("select * from final_df where track == '빌리지 고가의 질주'")
print((village.count(), len(village.columns))) #총 360571개. 
village.display()

# COMMAND ----------

import numpy as np
def cut_quantile(x, lower, upper):

#     """
#     x      : (pd.Series)
#     lower  : (float) lower bound to cut off
#     upper  : (float) upper bound to cut off
#     """
    x_quantile = x.quantile([lower, upper])
    lower_bound, upper_bound = x_quantile.values
    
    if x.min() >= 0:
        lower_bound = -np.finfo(float).eps   
    
    return x[(x > lower_bound) & (x < upper_bound)]


# COMMAND ----------

village2 = village.select("*").toPandas()

# COMMAND ----------

village3 = village2.playTime.values.tolist()

# COMMAND ----------

cut_quantile(village2['playTime'], 0.01, 0.99).isnull().sum()

# COMMAND ----------

village2['playTime'].quantile(0.99)

# COMMAND ----------

village2['playTime'].quantile(0.01)

# COMMAND ----------

village2[(village2['playTime'] < 103) | (village2['playTime'] > 173)] #4302/360571