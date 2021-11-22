# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionD_labeled.csv"))

df.registerTempTable("df")

# COMMAND ----------

tmp = spark.sql('''select p_accountNo,p_matchTime, channelName, gameSpeed, teamPlayers, p_rankinggrade2, difficulty, actualdistance, retiredRatio, playTime, label from df order by random() limit 10000  ''')

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = tmp.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("label",col("label").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))

print(df2.dtypes)

# COMMAND ----------

panda = df2.toPandas()
panda.corr()

# COMMAND ----------

from pathlib import Path
 
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.linear_model import LinearRegression
 
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.stats.outliers_influence import OLSInfluence
 
from pygam import LinearGAM, s, l
from pygam.datasets import wage
 
import seaborn as sns
import matplotlib.pyplot as plt
 
import plotly.express as px

# COMMAND ----------

panda3 = panda.copy()
from sklearn.preprocessing import MinMaxScaler
minscaler = MinMaxScaler()
numeric_data = ['actualdistance', 'retiredRatio', 'playTime'] ### actual distance와 retired Ratio ..... scaling을 시켜주어야할까요?
panda3[numeric_data] = minscaler.fit(panda3[numeric_data]).transform(panda3[numeric_data])
feature = [x for x in panda3.columns if (x != 'p_accountNo') & (x != 'playTime') & (x!='p_matchTime') & (x!='label')]
val = ""
cnt = 0 
for i in feature:
  if cnt != 0:
    val +=  "+" + i
    cnt += 1
  else:
    val += i
    cnt += 1
form = "p_matchTime ~" + val
form

# COMMAND ----------

import statsmodels.formula.api as sm
log_data2 = panda3
log_data2['p_matchTime'] = np.log1p(log_data2['p_matchTime']) 
result4 = sm.ols(formula = form, data = log_data2 ).fit()
print(result4.summary())

# COMMAND ----------

result4

# COMMAND ----------

form = 'p_matchTime ~channelName+gameSpeed+teamPlayers+p_rankinggrade2+difficulty'

# COMMAND ----------

result4 = sm.ols(formula = form, data = log_data2 ).fit()
print(result4.summary())

# COMMAND ----------

predictors = ['channelName', 'gameSpeed', 'teamPlayers','p_rankinggrade2', 'difficulty', 'actualdistance', 'retiredRatio']
target = 'p_matchTime'

# COMMAND ----------

influence = result4.get_influence()
pd.Series(influence.hat_matrix_diag).describe()

# COMMAND ----------

influence = result4.get_influence()
inf_sum = influence.summary_frame()

print(inf_sum.head())

# COMMAND ----------

student_resid = influence.resid_studentized_external
(cooks, p) = influence.cooks_distance
(dffits, p) = influence.dffits
leverage = influence.hat_matrix_diag

print ('\n')
print ('Leverage vs. Studentized Residuals')
sns.regplot(leverage, result4.resid_pearson,  fit_reg=False)
plt.title('Leverage vs. Studentized Residuals')
plt.xlabel('Leverage')
plt.ylabel('Studentized Residuals')

# COMMAND ----------

#Concat MEDV and the resulting residual table
#Note that hat_diag is leverage so change the ciolumn heading from hat_diag to leverage
from statsmodels.formula.api import ols

pmatchtimeres = pd.concat([panda3.p_matchTime, inf_sum], axis = 1)
pmatchtimeres=pmatchtimeres.rename(columns={'hat_diag': 'leverage'})
pmatchtimeres.head()

# COMMAND ----------

#studentized residuals for identifying outliers
#requested studentized residuals call them r
#studentized residuals that exceed +2 or -2 are concerning
#studentized residuals that exceed +3 or -3 are extremely concerning

r = pmatchtimeres.student_resid
print ('-'*30 + ' studentized residual ' + '-'*30)
print (r.describe())
print ('\n')

r_sort = pmatchtimeres.sort_values(by = 'student_resid')
print ('-'*30 + ' top 5 most negative residuals ' + '-'*30)
print (r_sort.head())
print ('\n')

print ('-'*30 + ' top 5 most positive residuals ' + '-'*30)
print (r_sort.tail())

# COMMAND ----------

#Print all MEDV values where the studentized residuals exceed 2
print (pmatchtimeres.p_matchTime[abs(r) > 2])
l = pmatchtimeres.leverage

# COMMAND ----------

#Cook's D of more than 3 times the mean is a possible outlier
#MEDVres.loc[:,"cooks_d"].mean()
cutoff=(pmatchtimeres.loc[:,"cooks_d"].mean())*3
outlier2=pd.DataFrame((pmatchtimeres.p_matchTime[abs(pmatchtimeres.cooks_d) > cutoff]))
print(outlier2)

# COMMAND ----------

pmatchtimeres.shape

# COMMAND ----------

pmatchtimeres['outcome'] = ""

# COMMAND ----------

pmatchtimeres['outcome'] = abs(pmatchtimeres['cooks_d']).apply(lambda x : 1 if x > cutoff else 0)

# COMMAND ----------

pmatchtimeres['outcome'][36]

# COMMAND ----------

pmatchtimeres

# COMMAND ----------

log_data2

# COMMAND ----------

import pandas as pd
temp = pd.concat([log_data2, pmatchtimeres], axis=1)
temp

# COMMAND ----------

temp['outcome'][38]

# COMMAND ----------

temp['outcome'][37]

# COMMAND ----------

type(temp['label'][0])

# COMMAND ----------

confusion_matrix = pd.crosstab(temp['label'], temp['outcome'], rownames=['Actual'], colnames=['Predicted'])
print (confusion_matrix)

# COMMAND ----------

!pip install pandas==0.23.4

# COMMAND ----------

label_list = temp['label'].tolist()

# COMMAND ----------

len(label_list)

# COMMAND ----------

outcome_list = temp['outcome'].tolist()
len(outcome_list)

# COMMAND ----------

import pandas as pd
from sklearn.metrics import confusion_matrix

pred = pd.DataFrame()
pred["y"] = [1,2,3]
pred["PredictedLabel"] = ['1','2','3']
conf = confusion_matrix(temp["label"].astype(str), temp["outcome"].astype(str))
print(conf)

# COMMAND ----------

from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix

print(f1_score(temp["label"].astype(str), temp["outcome"].astype(str), average="macro"))
print(precision_score(temp["label"].astype(str), temp["outcome"].astype(str), average="macro"))
print(recall_score(temp["label"].astype(str), temp["outcome"].astype(str), average="macro"))  

# COMMAND ----------

from sklearn.metrics import confusion_matrix
confusion_matrix(temp["label"].astype(str), temp["outcome"].astype(str))

# COMMAND ----------

from sklearn.metrics import accuracy_score
accuracy_score(temp["label"].astype(str), temp["outcome"].astype(str))

# COMMAND ----------

print('specificity :', 1 - )

# COMMAND ----------

#### 이건 아닌 것 같습니다... 

from sklearn.metrics import classification_report


print(classification_report(temp["label"].astype(str), temp["outcome"].astype(str), target_names=['class 0', 'class 1']))

# COMMAND ----------

from sklearn.metrics import roc_curve

fpr, tpr, thresholds = roc_curve(temp["label"], temp["outcome"], pos_label = 1)
print(fpr, tpr, thresholds )

# COMMAND ----------

from sklearn.metrics import auc
auc(fpr, tpr)

# COMMAND ----------

from sklearn.metrics import roc_auc_score
roc_auc_score(temp["label"], temp["outcome"])

# COMMAND ----------

roc_auc = roc_auc_score(temp["label"], temp["outcome"])

# COMMAND ----------

from sklearn.metrics import classification_report
print(classification_report(temp["label"].astype(str), temp["outcome"].astype(str)))

# COMMAND ----------

# method I: plt
import matplotlib.pyplot as plt
plt.title('Receiver Operating Characteristic')
plt.plot(fpr, tpr, 'b', label = 'AUC = %0.2f' % roc_auc)
plt.legend(loc = 'lower right')
plt.plot([0, 1], [0, 1],'r--')
plt.xlim([0, 1])
plt.ylim([0, 1])
plt.ylabel('True Positive Rate')
plt.xlabel('False Positive Rate')
plt.show()