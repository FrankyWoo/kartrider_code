# Databricks notebook source
# MAGIC %md
# MAGIC 정상 범위 정의
# MAGIC   - histogram and boxplot : 
# MAGIC     * p_rankinggrade2 & playtime
# MAGIC     * p_rankinggrade2 & Win
# MAGIC     * p_rankinggrade2 & retired rate
# MAGIC   - hampel filter

# COMMAND ----------

#statistical test : histogram
final = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/2/3/final_df.csv"))

final.registerTempTable("final_df")


# COMMAND ----------

village = spark.sql("select * from final_df where track == '빌리지 고가의 질주'")
village.display()

# COMMAND ----------

print((village.count(), len(village.columns))) #총 360571개. 

# COMMAND ----------

zero = spark.sql("select * from final_df where p_rankinggrade2 == 0")
one = spark.sql("select * from final_df where p_rankinggrade2 == 1")
two = spark.sql("select * from final_df where p_rankinggrade2 == 2")
three = spark.sql("select * from final_df where p_rankinggrade2 == 3")
four = spark.sql("select * from final_df where p_rankinggrade2 == 4")
five = spark.sql("select * from final_df where p_rankinggrade2 == 5")
six = spark.sql("select * from final_df where p_rankinggrade2 == 6")

# COMMAND ----------

zero.display()

# COMMAND ----------

zero.registerTempTable("zero2")
one.registerTempTable("one2")
two.registerTempTable("two2")
three.registerTempTable("three2")
four.registerTempTable("four2")
five.registerTempTable("five2")
six.registerTempTable("six2")

# COMMAND ----------

print((zero.count(), len(zero.columns)))

# COMMAND ----------

zero.display()

# COMMAND ----------

print(spark.sql("select count(*) from zero2 where playTime < 46 or playTime > 190.0").display())
print(spark.sql("select * from zero2 where playTime < 46 or playTime > 190.0").display())

# COMMAND ----------

zero3 = spark.sql("select * from zero2 where playTime < 46 or playTime > 190.0")
zero3.display()

# COMMAND ----------

one3 = spark.sql("select * from one2 where playTime < 43.5 or playTime > 191.5")
one3.display()

# COMMAND ----------

two3 = spark.sql("select * from two2 where playTime < 35.5 or playTime > 191.5")
two3.display()

# COMMAND ----------

three3 = spark.sql("select * from three2 where playTime < 51.5 or playTime > 175.5")
three3.display()

# COMMAND ----------

four3 = spark.sql("select * from four2 where playTime < 58.5 or playTime > 166.5")
four3.display()

# COMMAND ----------

five3 = spark.sql("select * from five2 where playTime < 54.5 or playTime > 170.5")
five3.display()

# COMMAND ----------

six3 = spark.sql("select * from six2 where playTime < 62.5 or playTime > 170.5")
six3.display()

# COMMAND ----------

print((zero3.count(), len(zero3.columns)))

# COMMAND ----------

print((one3.count(), len(one3.columns)))

# COMMAND ----------

print((two3.count(), len(two3.columns)))

# COMMAND ----------

print((three3.count(), len(three3.columns)))

# COMMAND ----------

print((four3.count(), len(four3.columns)))

# COMMAND ----------

print((five3.count(), len(five3.columns)))

# COMMAND ----------

print((six3.count(), len(six3.columns)))

# COMMAND ----------

zero4 = zero3.select("*").toPandas()
one4 = one3.select("*").toPandas()
two4 = two3.select("*").toPandas()
three4 = three3.select("*").toPandas()
four4 = four3.select("*").toPandas()
five4 = five3.select("*").toPandas()
six4 = six3.select("*").toPandas()

# COMMAND ----------

zero4 = zero4.p_accountNo.values.tolist()
one4 = one4.p_accountNo.values.tolist()
two4 = two4.p_accountNo.values.tolist()
three4 = three4.p_accountNo.values.tolist()
four4 = four4.p_accountNo.values.tolist()
five4 = five4.p_accountNo.values.tolist()
six4 = six4.p_accountNo.values.tolist()

# COMMAND ----------

playtimelicense = zero4 + one4 + two4 + three4 + four4 + five4 + six4
len(playtimelicense)

# COMMAND ----------

import pandas as pd
df = pd.DataFrame (playtimelicense, columns = ['p_accountNo'])
print (df)

# COMMAND ----------

df.to_csv('/dbfs/FileStore/KartRider/licenseplaytime.csv')

# COMMAND ----------

aa = pd.read_csv('/dbfs/FileStore/KartRider/licenseplaytime.csv')

# COMMAND ----------

for x in aa['p_accountNo'][:50]:
  print(x)

# COMMAND ----------

spark.sql("select count(*) from one2 where playTime < 43.5 or playTime > 191.5").display()
spark.sql("select count(*) from two2 where playTime < 46 or playTime > 190.0").display()

# COMMAND ----------

# MAGIC %matplotlib inline
# MAGIC sns.set(rc={'figure.figsize':(11.7,8.27)})
# MAGIC sns.boxplot(x="p_rankinggrade2", y="playTime", data=final2)

# COMMAND ----------

final2['playTime'].describe()

# COMMAND ----------

final2.groupby("p_rankinggrade2").describe()

# COMMAND ----------

final2.groupby("p_rankinggrade2")['playTime']

# COMMAND ----------

sns.scatterplot(data=final2, x="p_rankinggrade2", y="playTime")

# COMMAND ----------

final2.groupby('p_rankinggrade2')['playTime']

# COMMAND ----------

Q1 = final2.groupby('p_rankinggrade2')['playTime'].quantile(0.25)
Q3 = final2.groupby('p_rankinggrade2')['playTime'].quantile(0.75)
IQR = Q3 - Q1
print(IQR)

# COMMAND ----------

print(Q1 - 1.5 * IQR)

# COMMAND ----------

print(Q3 + 1.5 * IQR)

# COMMAND ----------

print(Q1 - 1.5 * IQR , Q3 + 1.5 * IQR)

# COMMAND ----------

spark.sql("select * from final_df where playTime < 11").display()

# COMMAND ----------

playab = spark.sql("select * from final_df where playTime < 12 order by startTime")
playab.display()

# COMMAND ----------

playabdf = playab.select("*").toPandas()

# COMMAND ----------

len(playabdf['matchId'].value_counts())

# COMMAND ----------

playabdf['matchId'].value_counts()

# COMMAND ----------

playabdf['channelName'].value_counts()

# COMMAND ----------

playabdf['playTime'].value_counts()

# COMMAND ----------

spark.sql("select * from final_df where p_rankinggrade2 == 0 and playTime < 46").display()

# COMMAND ----------

spark.sql("select * from final_df where p_rankinggrade2 == 0 and playTime > 190").display()

# COMMAND ----------

def is_outlier(s):
    lower_limit = s.mean() - (s.std() * 3)
    upper_limit = s.mean() + (s.std() * 3)
    return ~s.between(lower_limit, upper_limit)

final2.groupby('p_rankinggrade2')['playTime'].apply(is_outlier)

# COMMAND ----------

import numpy as np 

def get_outlier(df=None, column=None, weight=1.5):
  # target 값과 상관관계가 높은 열을 우선적으로 진행
  quantile_25 = np.percentile(df[column].values, 25)
  quantile_75 = np.percentile(df[column].values, 75)

  IQR = quantile_75 - quantile_25
  IQR_weight = IQR*weight
  
  lowest = quantile_25 - IQR_weight
  highest = quantile_75 + IQR_weight
  
  outlier_idx = df[column][ (df[column] < lowest) | (df[column] > highest) ].index
  return outlier_idx

# 함수 사용해서 이상치 값 삭제
oulier_idx = get_outlier(df=final2, column='playTime', weight=1.5)
#df.drop(outlier_idx, axis=0, inplace=True)

# COMMAND ----------

len(oulier_idx) #playtime에 대한 outlier 개수

# COMMAND ----------

def cut(mydata):
  IQR = mydata.quantile(0.75) - mydata.quantile(0.25)
  return pd.DataFrame([mydata.quantile(0.25)-(1.5*IQR),mydata.quantile(0.75)+(1.5*IQR)]*mydata.shape[0], columns = ['lower', 'upper'], index = mydata.index)

# COMMAND ----------

def outlier_iqr(data, column): 

    # lower, upper 글로벌 변수 선언하기     
    global lower, upper    
    
    # 4분위수 기준 지정하기     
    q25, q75 = np.quantile(data[column], 0.25), np.quantile(data[column], 0.75)          
    
    # IQR 계산하기     
    iqr = q75 - q25    
    
    # outlier cutoff 계산하기     
    cut_off = iqr * 1.5          
    
    # lower와 upper bound 값 구하기     
    lower, upper = q25 - cut_off, q75 + cut_off     
    
    print('IQR은',iqr, '이다.')     
    print('lower bound 값은', lower, '이다.')     
    print('upper bound 값은', upper, '이다.')    
    
    # 1사 분위와 4사 분위에 속해있는 데이터 각각 저장하기     
    data1 = data[data[column] > upper]     
    data2 = data[data[column] < lower]    
    
    # 이상치 총 개수 구하기
    return print('총 이상치 개수는', data1.shape[0] + data2.shape[0], '이다.')
    

# COMMAND ----------

outlier_iqr(final2, 'playTime')

# COMMAND ----------

license = spark.sql("select p_rankinggrade2, playtime, p_matchRetired, p_matchRank, p_matchTime, p_matchWin from final_df")
license.display()

# COMMAND ----------

license.registerTempTable("license_df")

# COMMAND ----------

spark.sql("select p_rankinggrade2, count(p_rankinggrade2) from license_df group by p_rankinggrade2 order by p_rankinggrade2").display()

# COMMAND ----------

df = license.select("*").toPandas()

# COMMAND ----------

df.display()

# COMMAND ----------

#랭킹 그레이드별 평균을 기준
license2 = spark.sql("select p_rankinggrade2, avg(playtime) from final_df group by p_rankinggrade2")
license2.display()

# COMMAND ----------

license2.display()

# COMMAND ----------

zero = spark.sql("select playtime from final_df where p_rankinggrade2 == '0'")

# COMMAND ----------

import pandas as pd
df = final.select("*").toPandas()

# COMMAND ----------

df.columns

# COMMAND ----------

!pip install cufflinks

# COMMAND ----------

import matplotlib
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
from IPython.display import HTML
import plotly.express as px
#rom plotly.offline.offline import _plot_html
import numpy as np
import cufflinks as cf



zero7 =  zero.select("*").toPandas()
one7 =  one.select("*").toPandas()
two7 =  two.select("*").toPandas()
three7 =  three.select("*").toPandas()
four7 =  four.select("*").toPandas()
five7 =  five.select("*").toPandas()
six7 =  six.select("*").toPandas()

# COMMAND ----------

!pip install outlier_utils

# COMMAND ----------

zero_playtime = zero7.playTime.values.tolist()
zero_track = zero7.track.values.tolist()


# COMMAND ----------

one_playtime = one7.playTime.values.tolist()
one_track = one7.track.values.tolist()

# COMMAND ----------

import numpy as np
from outliers import smirnov_grubbs as grubbs

grubbs.test(zero_playtime, alpha = .05)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats

# COMMAND ----------

def grubbs_stat(y):
    std_dev = np.std(y)
    avg_y = np.mean(y)
    abs_val_minus_avg = abs(y - avg_y)
    max_of_deviations = max(abs_val_minus_avg)
    max_ind = np.argmax(abs_val_minus_avg)
    Gcal = max_of_deviations/ std_dev
    print("Grubbs Statistics Value : {}".format(Gcal))
    return Gcal, max_ind

# COMMAND ----------


def calculate_critical_value(size, alpha):
    t_dist = stats.t.ppf(1 - alpha / (2 * size), size - 2)
    numerator = (size - 1) * np.sqrt(np.square(t_dist))
    denominator = np.sqrt(size) * np.sqrt(size - 2 + np.square(t_dist))
    critical_value = numerator / denominator
    print("Grubbs Critical Value: {}".format(critical_value))
    return critical_value

# COMMAND ----------

def check_G_values(Gs, Gc, inp, max_index):
    if Gs > Gc:
        print('{} is an outlier. G > G-critical: {:.4f} > {:.4f} \n'.format(inp[max_index], Gs, Gc))
    else:
        print('{} is not an outlier. G > G-critical: {:.4f} > {:.4f} \n'.format(inp[max_index], Gs, Gc))

# COMMAND ----------

def ESD_Test(input_series, alpha, max_outliers):
    for iterations in range(max_outliers):
        Gcritical = calculate_critical_value(len(input_series), alpha)
        Gstat, max_index = grubbs_stat(input_series)
        check_G_values(Gstat, Gcritical, input_series, max_index)
        input_series = np.delete(input_series, max_index)

# COMMAND ----------

six_playtime = six7.playTime.values.tolist()
six_track = six7.track.values.tolist()

# COMMAND ----------

x = one_track 
y = one_playtime
ESD_Test(y, 0.05, 100)


# COMMAND ----------

x = two_track 
y = two_playtime
ESD_Test(y, 0.05, 100)

# COMMAND ----------

x = three_track 
y = three_playtime
ESD_Test(y, 0.05, 100)

# COMMAND ----------

x = four_track 
y = four_playtime
ESD_Test(y, 0.05, 100)

# COMMAND ----------

x = five_track 
y = five_playtime
ESD_Test(y, 0.05, 100)

# COMMAND ----------

x = six_track 
y = six_playtime
ESD_Test(y, 0.05, 100)