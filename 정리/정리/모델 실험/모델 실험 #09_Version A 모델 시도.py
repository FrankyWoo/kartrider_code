# Databricks notebook source
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))

df.registerTempTable("df")

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### type 변경

# COMMAND ----------

### type 변경 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
import pyspark.sql.functions as func
## difficulty
df2 = df.withColumn("p_rankinggrade2",col("p_rankinggrade2").cast(StringType())) \
    .withColumn("teamPlayers",col("teamPlayers").cast(StringType())) \
    .withColumn("p_matchRetired",col("p_matchRetired").cast(StringType()))\
    .withColumn("gameSpeed",col("gameSpeed").cast(StringType())) \
    .withColumn("difficulty", func.round(df["difficulty"]).cast(StringType()))


print(df2.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stats 사용을 위해 Pandas 변환 

# COMMAND ----------

panda = df2.toPandas()

# COMMAND ----------

del panda['rs']
del panda['trackId']
import pandas as pd
cat_columns = [c for c, t in zip(panda.dtypes.index, panda.dtypes) if t == 'O'] 
num_columns = [c for c    in panda.columns if c not in cat_columns]
print(cat_columns)
print(num_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standard Scaling for numerical values 

# COMMAND ----------

panda[num_columns]

# COMMAND ----------

num_cols = ['playTime', 'actualdistance', 'retiredRatio' ]

# COMMAND ----------

numeric_data = panda[num_cols].values
numeric_data

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaler.fit(numeric_data)

# COMMAND ----------

print(scaler.mean_)

# COMMAND ----------

panda[num_cols] = scaler.transform(panda[num_cols])

# COMMAND ----------

panda

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log transformation 

# COMMAND ----------

import numpy as np
np.log1p(panda['p_matchTime']) 

# COMMAND ----------

np.log(panda['p_matchTime']) 

# COMMAND ----------

panda['p_matchTime'] = np.log1p(panda['p_matchTime']) 
panda['p_matchTime']

# COMMAND ----------

panda['p_matchTime'].hist(bins=20)

# COMMAND ----------

panda[num_columns]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Label Encoding 

# COMMAND ----------

import mlflow.sklearn
mlflow.sklearn.autolog()

# COMMAND ----------



# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
panda[cat_columns] = panda[cat_columns].apply(le.fit_transform)

# COMMAND ----------

panda

# COMMAND ----------

# MAGIC %md
# MAGIC One Hot Encoding

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder

ohe = OneHotEncoder()

#One-hot-encode the categorical columns.
#Unfortunately outputs an array instead of dataframe.
array_hot_encoded = ohe.fit_transform(panda[cat_columns])

#Convert it to df
data_hot_encoded = pd.DataFrame(array_hot_encoded, index=panda.index)

#Extract only the columns that didnt need to be encoded
data_other_cols = panda.drop(columns=cat_columns)

#Concatenate the two dataframes : 
data_out = pd.concat([data_hot_encoded, data_other_cols], axis=1)

# COMMAND ----------

data_out

# COMMAND ----------



# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder
onehotencoder = OneHotEncoder()

transformed_data = onehotencoder.fit_transform(panda[cat_columns])

# the above transformed_data is an array so convert it to dataframe
encoded_data = pd.DataFrame(transformed_data, index=panda.index)

# now concatenate the original data and the encoded data using pandas
concatenated_data = pd.concat([panda, encoded_data], axis=1)

# COMMAND ----------

concatenated_data

# COMMAND ----------

# Rename the columns of df2_melted: df2_tidy
data_out.rename(columns = {0: 'cat_feature'}, inplace = True)

# COMMAND ----------

data_out

# COMMAND ----------

# MAGIC %md
# MAGIC #### dummies

# COMMAND ----------

import pandas as pd
pd.concat((panda,pd.get_dummies(panda, columns=cat_columns, drop_first=True)), axis=1) #model에 drop 여부 확인

# COMMAND ----------

panda

# COMMAND ----------

data = pd.get_dummies(panda, columns=cat_columns, drop_first=True)
data

# COMMAND ----------

del data['playTime']

# COMMAND ----------



# COMMAND ----------

feature = [x for x in data.columns if x != 'p_matchTime']

# COMMAND ----------

feature

# COMMAND ----------

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

# COMMAND ----------

form

# COMMAND ----------

import statsmodels.formula.api as sm
result = sm.ols(formula = form, data = data ).fit()

# COMMAND ----------

print(result.params)

# COMMAND ----------

print(result.summary())

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Backward elimination </br>
# MAGIC https://blog.naver.com/PostView.nhn?blogId=jaehong7719&logNo=221909615639

# COMMAND ----------

form

# COMMAND ----------

form2 = 'p_matchTime ~actualdistance+retiredRatio+channelName_1+channelName_2+channelName_3+channelName_4+channelName_5+channelName_7+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+teamPlayers_1+teamPlayers_2+teamPlayers_3+difficulty_1+difficulty_2+difficulty_3+difficulty_4+difficulty_5+p_matchRetired_1+track_1+track_2+track_3+track_4+track_5+track_6+track_7+track_8+track_9+track_10+track_11+track_12+track_13+track_14+track_15+track_16+track_17+track_18+track_19+track_20+track_21+track_22+track_23+track_24+track_25+track_26+track_27+track_28+track_29+track_30+track_31+track_32+track_33+track_34+track_35+track_36+track_37+track_38+track_39+track_40+track_41+track_42+track_43+track_44+track_45+track_46+track_47+track_48+track_49+track_50+track_51+track_52+track_53+track_54+track_55+track_56+track_57+track_58+track_59+track_60+track_61+track_62+track_63+track_64+track_65+track_66+track_67+track_68+track_69+track_70+track_71+track_72+track_73+track_74+track_75+track_76+track_77+track_78+track_79+track_80+track_81+track_82+track_83+track_84+track_85+track_86+track_87+track_88+track_89+track_90+track_91+track_92+track_93+track_94+track_95+track_96+track_97+track_98+track_99+track_100+track_101+track_102+track_103+track_104+track_105+track_106+track_107+track_108+track_109+track_110+track_111+track_112+track_113+track_114+track_115+track_116+track_117+track_118+track_119+track_120+track_121+track_122+track_123+track_124+track_125+track_126+track_127+track_128+track_129+track_130+track_131+track_132+track_133+track_134+track_135+track_136+track_137+track_138+track_139+track_140+track_141+track_142+track_143+track_144+track_145+track_146+track_147+track_148+track_149+track_150+track_151+track_152+track_153+track_154+track_155+track_156+track_157+track_158+track_159+track_160+track_161+track_162+track_163+track_164+track_165+track_166+track_167+track_168+track_169+track_170+track_171+track_172+track_173+track_174+track_175+track_176+track_177+track_178+track_179+track_180+track_181+track_182+track_183+track_184+track_185+track_186+track_187+track_188+track_189+track_190+track_191+track_192+track_193+track_194+track_195+track_196+track_197+track_198+track_199+track_200+track_201+track_202+track_203+track_204+track_205+track_206+track_207+track_208+track_209+track_210+track_211+track_212+track_213+track_214+track_215+track_216+track_217+track_218+track_219+track_220+track_221+track_222+track_223+track_224+track_225+track_226+track_227+track_228+track_229+track_230+track_231+track_232+track_233+track_234+track_235+track_236+track_237+track_238+track_239+track_240+track_241+track_242+track_243+track_244+track_245+track_246+track_247+track_248+track_249+track_250+track_251+track_252+track_253+track_254+track_255+track_256+track_257+track_258+track_259+track_260+track_261+track_262+track_263+track_264+track_265+track_266+track_267+track_268+track_269+track_270+track_271+track_272+track_273+track_274+track_275+track_276+track_277+track_278+track_279+track_280+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4'

# COMMAND ----------

result = sm.ols(formula = form2, data = data ).fit()
print(result.summary())

# COMMAND ----------

form3 = 'p_matchTime ~actualdistance+retiredRatio+channelName_1+channelName_2+channelName_4+channelName_5+channelName_7+p_rankinggrade2_1+p_rankinggrade2_2+p_rankinggrade2_3+p_rankinggrade2_4+p_rankinggrade2_5+p_rankinggrade2_6+teamPlayers_1+teamPlayers_2+teamPlayers_3+difficulty_1+difficulty_2+difficulty_3+difficulty_4+difficulty_5+p_matchRetired_1+track_1+track_2+track_3+track_4+track_5+track_6+track_7+track_8+track_9+track_10+track_11+track_12+track_13+track_14+track_15+track_16+track_17+track_18+track_19+track_20+track_21+track_22+track_23+track_24+track_25+track_26+track_27+track_28+track_29+track_30+track_31+track_32+track_33+track_34+track_35+track_36+track_37+track_38+track_39+track_40+track_41+track_42+track_43+track_44+track_45+track_46+track_47+track_48+track_49+track_50+track_51+track_52+track_53+track_54+track_55+track_56+track_57+track_58+track_59+track_60+track_61+track_62+track_63+track_64+track_65+track_66+track_67+track_68+track_69+track_70+track_71+track_72+track_73+track_74+track_75+track_76+track_77+track_78+track_79+track_80+track_81+track_82+track_83+track_84+track_85+track_86+track_87+track_88+track_89+track_90+track_91+track_92+track_93+track_94+track_95+track_96+track_97+track_98+track_99+track_100+track_101+track_102+track_103+track_104+track_105+track_106+track_107+track_108+track_109+track_110+track_111+track_112+track_113+track_114+track_115+track_116+track_117+track_118+track_119+track_120+track_121+track_122+track_123+track_124+track_125+track_126+track_127+track_128+track_129+track_130+track_131+track_132+track_133+track_134+track_135+track_136+track_137+track_138+track_139+track_140+track_141+track_142+track_143+track_144+track_145+track_146+track_147+track_148+track_149+track_150+track_151+track_152+track_153+track_154+track_155+track_156+track_157+track_158+track_159+track_160+track_161+track_162+track_163+track_164+track_165+track_166+track_167+track_168+track_169+track_170+track_171+track_172+track_173+track_174+track_175+track_176+track_177+track_178+track_179+track_180+track_181+track_182+track_183+track_184+track_185+track_186+track_187+track_188+track_189+track_190+track_191+track_192+track_193+track_194+track_195+track_196+track_197+track_198+track_199+track_200+track_201+track_202+track_203+track_204+track_205+track_206+track_207+track_208+track_209+track_210+track_211+track_212+track_213+track_214+track_215+track_216+track_217+track_218+track_219+track_220+track_221+track_222+track_223+track_224+track_225+track_226+track_227+track_228+track_229+track_230+track_231+track_232+track_233+track_234+track_235+track_236+track_237+track_238+track_239+track_240+track_241+track_242+track_243+track_244+track_245+track_246+track_247+track_248+track_249+track_250+track_251+track_252+track_253+track_254+track_255+track_256+track_257+track_258+track_259+track_260+track_261+track_262+track_263+track_264+track_265+track_266+track_267+track_268+track_269+track_270+track_271+track_272+track_273+track_274+track_275+track_276+track_277+track_278+track_279+track_280+gameSpeed_1+gameSpeed_2+gameSpeed_3+gameSpeed_4'

# COMMAND ----------

result = sm.ols(formula = form3, data = data ).fit()
print(result.summary())