# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA_id.csv"))

df.registerTempTable("df")

# COMMAND ----------

panda3 = df.toPandas()

# COMMAND ----------

panda3

# COMMAND ----------

test2 = log_data2.sample(n=10000, random_state=1004)


# COMMAND ----------

test

# COMMAND ----------

matchtime2 = test["p_matchTime"].values

# COMMAND ----------

f = Fitter(matchtime2,
           distributions=['gamma',
                          'lognorm',
                          "beta",
                          "burr",
                          "norm"])
f.fit()
f.summary()

# COMMAND ----------

panda = df.toPandas()

# COMMAND ----------

import numpy as np
log_data2 = panda.copy()
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 

# COMMAND ----------

panda

# COMMAND ----------

test.shape

# COMMAND ----------

matchtime3 = test["p_matchTime"].values

# COMMAND ----------

len(matchtime3)

# COMMAND ----------

matchtime3

# COMMAND ----------

f = Fitter(matchtime3,
           distributions=['gamma',
                          'lognorm',
                          "beta",
                          "burr",
                          "norm"])
f.fit()
f.summary()

# COMMAND ----------

matchtime4 = test2["p_matchTime"].values

f = Fitter(matchtime4,
           distributions=['gamma',
                          'lognorm',
                          "beta",
                          "burr",
                          "norm"])
f.fit()
f.summary()

# COMMAND ----------

test3= log_data2.sample(n=1000, random_state=1004)
matchtime5 = test3["p_matchTime"].values

f = Fitter(matchtime5,
           distributions=['gamma',
                          'lognorm',
                          "beta",
                          "burr",
                          "norm"])
f.fit()
f.summary()

# COMMAND ----------

f.get_best(method = 'sumsquare_error') ### matchtime2 원데이터에 대한 정규성. 

# COMMAND ----------

from scipy.special import boxcox1p
import numpy as np
log_data2 = panda
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 
print(log_data2["p_matchTime"].plot(kind="hist"))

# COMMAND ----------

### sampling을 통해 확인 
import numpy as np
import pandas as pd
import seaborn as sns
from fitter import Fitter, get_common_distributions, get_distributions

# COMMAND ----------

import plotly.express as px

fig = px.histogram(panda, x="p_matchTime")
fig.show()

# COMMAND ----------

matchtime = log_data2["p_matchTime"].values

# COMMAND ----------

f = Fitter(matchtime,
           distributions=['gamma',
                          'lognorm',
                          "beta",
                          "burr",
                          "norm"])
f.fit()
f.summary()

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+track'

# COMMAND ----------

### ols model
import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = log_data2).fit()
print(result.summary())

# COMMAND ----------

import statsmodels.api as sm
from matplotlib import pyplot as plt
import scipy.stats as stats
res = result.resid
fig = sm.qqplot(res, stats.t, fit=True, line="45")
plt.show()

# COMMAND ----------

from statsmodels.graphics.gofplots import qqplot
from matplotlib import pyplot
qqplot(log_data2['p_matchTime'], line='s')
pyplot.show()

# COMMAND ----------

# Shapiro-Wilk Test
from numpy.random import seed
from numpy.random import randn
from scipy.stats import shapiro

stat, p = shapiro(log_data2['p_matchTime'])
print('Statistics=%.3f, p=%.3f' % (stat, p))
# interpret
alpha = 0.05
if p > alpha:
	print('Sample looks Gaussian (fail to reject H0)')
else:
	print('Sample does not look Gaussian (reject H0)')