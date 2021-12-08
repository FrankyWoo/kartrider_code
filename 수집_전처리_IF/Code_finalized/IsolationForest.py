# Databricks notebook source
#데이터 로드 

import pandas as pd


df = (spark.read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("/FileStore/KartRider/versionA.csv"))


df.registerTempTable("df")
temp=df.toPandas()

# COMMAND ----------

temp

# COMMAND ----------

#변수추출
test=temp[["p_matchTime","playTime","actualdistance","retiredRatio","p_rankinggrade2","difficulty","channelName","track","gameSpeed","teamPlayers"]]
feautres_df=test[["p_matchTime","playTime","actualdistance","difficulty","retiredRatio","p_rankinggrade2","teamPlayers"]]
# label_df=test[["label"]]

#트랙,채널에 대해 라벨인코딩
from sklearn.preprocessing import LabelEncoder
le=LabelEncoder()
test["track_le"]=le.fit_transform(temp["track"])
test["channel_le"]=le.fit_transform(temp["channelName"])

#get_dummies로 변수 생성 => dtype int를 씀으로써 오류방지
track_encoded=pd.get_dummies(test["track_le"], dtype='int32')
channel_encoded=pd.get_dummies(test["channel_le"], dtype='int32')
speed_encoded=pd.get_dummies(test["gameSpeed"], dtype='int32')

#get_dummies를 하면서 변수명이 모두 정수형으로 생성됏으므로 채널과 스피드 변수에 대해 변수명 변경
channel_encoded.rename(columns={0:'channel_0',1:'channel_1', 2:'channel_2',3:'channel_3',4:'channel_4',5:'channel_5',6:'channel_6',7:'channel_7'}, inplace = True)
speed_encoded.rename(columns={0:'speed_0',1:'speed_1',2:'speed_2',3:'speed_3',4:'speed_4',7:'speed2'},inplace=True)

verA=pd.concat([feautres_df,track_encoded,channel_encoded,speed_encoded],axis=1)

# COMMAND ----------

verA

# COMMAND ----------

#실질적인 isolation forest 모델
# verA는 학습시킬 데이터셋
# y=verA.copy()를 통해 변수의 혼동을 막음 

from sklearn.ensemble import IsolationForest

#기본변수와 트랙,채널,스피드 정보 결합
verA=pd.concat([feautres_df,track_encoded,channel_encoded,speed_encoded],axis=1)

#학습데이터
train_data=verA

#x는 최종 결과(이상치스코어,라벨링 등)를 담을 데이터프레임
x=verA.copy()

#ratio는 불순도 리스트
ratio=[0.075]

for i in ratio:
  
    #모델생성(파라미터투입)
    if_sk=IsolationForest(n_estimators=50,max_samples=200,contamination=i, random_state=123, max_features=0.25)
    
    
    #학습
    if_sk.fit(train_data.to_numpy())
    
    
    #이상치 여부를 예측하고, 여부에 따른 라벨링 -1:이상치
    y_pred=if_sk.predict(train_data.to_numpy())
    y_pred=[1 if x==-1 else 0 for x in y_pred]
    
    #점수를 매기는 과정(싸이킷런에서 제공하는 점수가 기존의 점수와는 달라 original_paper_score로 변경해줌
    score=if_sk.decision_function(train_data.to_numpy())
    original_paper_score = [-1*s + 0.5 for s in score]
    
    #점수와 이상치 라벨을 x에 넣어줌
    x["score"+str(i)]=original_paper_score
    x["anomal_clustered_label"+str(i)]=y_pred
    
    #이상치/정상치 및 이상치/정상치 스코어 통계값
    print("contamination:"+str(i)+" ,"+"이상치 최대값:"+" "+ str(x[x["anomal_clustered_label"+str(i)]==1]["p_matchTime"].max()))
    print("contamination:"+str(i)+" ,"+"이상치 최소값:"+" "+ str(x[x["anomal_clustered_label"+str(i)]==1]["p_matchTime"].min()))
    print("contamination:"+str(i)+" ,"+"이상치 평균:"+" "+str(x[x["anomal_clustered_label"+str(i)]==1]["p_matchTime"].mean()))
   

    print("contamination:"+str(i)+" ,"+"정상치 최댓값:"+" "+ str(x[x["anomal_clustered_label"+str(i)]==0]["p_matchTime"].max()))
    print("contamination:"+str(i)+" ,"+"정상치 최소값:"+" "+str(x[x["anomal_clustered_label"+str(i)]==0]["p_matchTime"].min()))
    print("contamination:"+str(i)+" ,"+"정상치 평균:"+" "+str(x[x["anomal_clustered_label"+str(i)]==0]["p_matchTime"].mean()))
    
    
   
    print("이상치 score의 평균:"+""+str(x[x["anomal_clustered_label"+str(i)]==1]["score"+str(i)].mean()))
    print("정상치 score의 평균:"+""+str(x[x["anomal_clustered_label"+str(i)]==0]["score"+str(i)].mean()))

  
    print("\n")

# COMMAND ----------

x