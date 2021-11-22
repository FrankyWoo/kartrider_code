# Databricks notebook source
pearson = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/correlation.csv"))

pearson.registerTempTable("pearson")

# COMMAND ----------

pearson.display()

# COMMAND ----------

tmp = spark.sql("select track_list, round(r_value, 3) as pearson from pearson ")
tmp.display()

# COMMAND ----------

import numpy as np
import plotly.figure_factory as ff

### densityy graph
fig = create_distplot(
    hist_data, group_labels,curve_type='normal',
    show_rug=False, bin_size=10)

fig['layout'].update(width=1080)
fig['layout'].update(height=720)
fig.show()()

# COMMAND ----------

coeff = tmp.select("pearson").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

coeff

# COMMAND ----------

from plotly.figure_factory import create_distplot
group_labels = ['Relationship between matchTime and playtime']
fig = create_distplot([coeff], group_labels)
fig.show()

# COMMAND ----------

tmp2 = spark.sql("select track_list, round(r_value, 2) as pearson from pearson ")
tmp2.display()

# COMMAND ----------

from plotly.figure_factory import create_distplot
coeff2 = tmp2.select("pearson").rdd.flatMap(lambda x: x).collect()
group_labels = ['Relationship between matchTime and playtime']
fig = create_distplot([coeff2], group_labels)
fig.show()

# COMMAND ----------

import plotly.figure_factory as ff
import numpy as np

hist_data = [coeff2]
group_labels = ['distplot'] # name of the dataset

fig = ff.create_distplot(hist_data, group_labels)
fig.show()

# COMMAND ----------

import plotly.express as px
#tmp4 = tmp.toPandas()
fig = px.histogram(tmp4, x="pearson")
fig['layout'].update(height=720)
fig.show()

# COMMAND ----------

fig = px.histogram(tmp4, x="pearson")
fig['layout'].update(xaxis=dict(range=[0.8, 1]))
fig.show()

# COMMAND ----------

import plotly.express as px
tmp3 = tmp2.toPandas()
fig = px.histogram(tmp3, x="pearson")
fig.show()

# COMMAND ----------

pearson = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/correlation.csv"))

pearson.registerTempTable("pearson")