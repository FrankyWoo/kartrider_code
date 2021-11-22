# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA_id.csv"))

df.registerTempTable("df")

# COMMAND ----------

panda = df.toPandas()

# COMMAND ----------

panda

# COMMAND ----------

print(panda["p_matchTime"].plot(kind="hist"))

# COMMAND ----------

from scipy.special import boxcox1p
lam = 0.1
panda["p_matchTime"] = boxcox1p(panda["p_matchTime"], lam)
print(panda["p_matchTime"].plot(kind="hist"))

# COMMAND ----------

### sampling을 통해 확인 
import numpy as np
import pandas as pd
import seaborn as sns
from fitter import Fitter, get_common_distributions, get_distributions

# COMMAND ----------

matchtime = panda["p_matchTime"].values

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

f.get_best(method = 'sumsquare_error')

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty+track'

# COMMAND ----------

### ols model
import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = panda).fit()
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
qqplot(panda['p_matchTime'], line='s')
pyplot.show()

# COMMAND ----------

# Shapiro-Wilk Test
from numpy.random import seed
from numpy.random import randn
from scipy.stats import shapiro

stat, p = shapiro(panda['p_matchTime'])
print('Statistics=%.3f, p=%.3f' % (stat, p))
# interpret
alpha = 0.05
if p > alpha:
	print('Sample looks Gaussian (fail to reject H0)')
else:
	print('Sample does not look Gaussian (reject H0)')

# COMMAND ----------

from scipy.stats import anderson
anderson(panda['p_matchTime'])