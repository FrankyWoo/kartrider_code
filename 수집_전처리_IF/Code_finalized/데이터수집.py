# Databricks notebook source
import multiprocessing
num_cores = multiprocessing.cpu_count()
num_cores #2

# COMMAND ----------

# 하루를 1분단위로 쪼개는 작업

from datetime import datetime, timedelta, date, time
timedata = []
dt1 = datetime(2021,9,30,0,0,0)
dt2 = datetime(2021,9,30,0,1,0)
delta = timedelta(minutes =1)
while dt1.strftime("%d") == '30':
    temp=[]
    temp.append(str(dt1))
    temp.append(str(dt2))
    timedata.append(temp)
    dt1 += delta
    dt2 += delta


# COMMAND ----------

# 사용할 코어수(6)만큼 시간을 쪼개 리스트로 저장(6개원소)

import numpy as np
splited_timedata =  np.array_split(timedata, 6)
splited_timedata = [x.tolist() for x in splited_timedata]

# COMMAND ----------

# api key

my_token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2NvdW50X2lkIjoiMTk0NjI5NDU3OSIsImF1dGhfaWQiOiIyIiwidG9rZW5fdHlwZSI6IkFjY2Vzc1Rva2VuIiwic2VydmljZV9pZCI6IjQzMDAxMTM5MyIsIlgtQXBwLVJhdGUtTGltaXQiOiI1MDA6MTAiLCJuYmYiOjE2MzI3MTAwNTQsImV4cCI6MTY0ODI2MjA1NCwiaWF0IjoxNjMyNzEwMDU0fQ.6uXnG-Ay2e-aTxJh9vtoiY38Kl1LYYuWhY2hEs8zKpc'

# COMMAND ----------

#설정한 시간만큼의 매치아이디 수집후 데이터프레임 작성하여 DBFS로 저장

import parmap
import multiprocessing
import requests
import time as ti
import pandas as pd



manager = multiprocessing.Manager()
matchidMay = manager.list()
st_time=manager.list()
en_time=manager.list()

def a(data):
    print("!!!!!!!!!!!!11",data)
    for x in data:

      url1 = "https://api.nexon.co.kr/kart/v1.0/matches/all?start_date={}&end_date={}&offset=0&limit=500&match_types=effd66758144a29868663aa50e85d3d95c5bc0147d7fdb9802691c2087f3416e".format(x[0],x[1])
  
      result = requests.get(url=url1, headers={'Authorization': my_token}).json()
      
      if len(result['matches']) != 0:
          result = result['matches'][0]['matches']
        
          for i in range(0, len(result)):
              st_time.append(x[0])
              en_time.append(x[1])
              matchidMay.append(result[i])
              
      ti.sleep(0.5)
            
    return matchidMay
            
parmap.map(a, splited_timedata, pm_pbar=True, pm_processes=6)

df = pd.DataFrame(data=zip(st_time,en_time,matchidMay),columns=['start','end','matchid'])
df.to_csv('/dbfs/FileStore/KartRider/2/2/2021_09_30_speed_mathchid.csv')




# COMMAND ----------

#매치아이디 수집 확인작업

import pandas as pd
pandas_df = pd.read_csv("/dbfs/FileStore/KartRider/2/2/2021_09_20_speed_mathchid.csv", header='infer') 
print(len(pandas_df["matchid"].unique()))
pandas_df

# COMMAND ----------

#수집된 매치아이디 데이터프레임을 리스트로 불러들임

matchid_2021_09_30_speed= pandas_df["matchid"].tolist()

# COMMAND ----------

#게임데이터 정보 수집을 위해 매치아이디를 사용코어의 개수(7)로 쪼갬

import numpy as np

splited_timedata =  np.array_split(matchid_2021_09_30_speed, 7)
len(splited_timedata[0])



# COMMAND ----------

# 매치아이디를 넣어 게임데이터 수집
# toatl_list에 게임데이터 정보를 하나씩 담아 로우별로 다시 어펜드하는 방식(기존의 데이터가 잘못 들어왔던 오류를 피하기 위함)

import parmap
import multiprocessing
import requests
import time as ti

 
num_cores = multiprocessing.cpu_count()
manager = multiprocessing.Manager()
channelName2 = manager.list()
endTime2 = manager.list()
gameSpeed2 = manager.list()
matchId2 = manager.list()
matchResult2 = manager.list() # vs win... 
matchType2 = manager.list()
playTime2 = manager.list()
startTime2 = manager.list()
trackId2 = manager.list()
teamId2 = manager.list()
teamPlayers2 = manager.list()
p_accountNo2 = manager.list()
p_characterName2 = manager.list()
p_character2 = manager.list()
p_kart2 = manager.list()
p_license2 = manager.list()
p_pet2 = manager.list()
p_flyingPet2 = manager.list()
p_partsEngine2 = manager.list()
p_partsHandle2 = manager.list()
p_partsWheel2 = manager.list()
p_partsKit2 = manager.list()
p_rankinggrade22 = manager.list()
p_matchRank2 = manager.list()
p_matchRetired2 = manager.list()
p_matchWin2 = manager.list()
p_matchTime2 = manager.list()

total_list=manager.list()
ex = manager.list()
ex_data=[]
ex_x=[] 

def a(data):
  try:
    
    for x in data:
      ti.sleep(0.7)
      url1 = "https://api.nexon.co.kr/kart/v1.0/matches/{}".format(x)
      r = requests.get(url=url1, headers={'Authorization': my_token}).json()
      
      for i in range(0,2):
        for j in range(0, len(r['teams'][i]['players'])):
          total_list.append({
          "channelName2":r['channelName'],
          "endTime2":r['endTime'],
          "gameSpeed2":r['gameSpeed'],
          "matchId2":r['matchId'], 
          "matchResult2":r['matchResult'],
          "matchType2":r['matchType'],
          "playTime2":r['playTime'],
          "startTime2":r['startTime'],
          "trackId2":r['trackId'],
          "teamId2":r['teams'][i]['teamId'],
          "teamPlayers2":len(r['teams'][i]['players']),
          "p_accountNo2":r['teams'][i]['players'][j]['accountNo'],
          "p_characterName2":r['teams'][i]['players'][j]['characterName'],
          "p_character2":r['teams'][i]['players'][j]['character'],
          "p_kart2":r['teams'][i]['players'][j]['kart'],
          "p_license2":r['teams'][i]['players'][j]['license'],
          "p_pet2":r['teams'][i]['players'][j]['pet'],
          "p_flyingPet2":r['teams'][i]['players'][j]['flyingPet'],
          "p_partsEngine2":r['teams'][i]['players'][j]['partsEngine'],
          "p_partsHandle2":r['teams'][i]['players'][j]['partsHandle'],
          "p_partsWheel2":r['teams'][i]['players'][j]['partsWheel'],
          "p_partsKit2":r['teams'][i]['players'][j]['partsKit'],
          "p_rankinggrade22":r['teams'][i]['players'][j]['rankinggrade2'],
          "p_matchRank2":r['teams'][i]['players'][j]['matchRank'],
          "p_matchRetired2":r['teams'][i]['players'][j]['matchRetired'],
          "p_matchWin2":r['teams'][i]['players'][j]['matchWin'],
          "p_matchTime2":r['teams'][i]['players'][j]['matchTime']
          })

            
parmap.map(a, splited_timedata, pm_pbar=True, pm_processes=7)

for row in total_list:
  channelName2.append(row['channelName2'])
  endTime2.append(row['endTime2'])
  gameSpeed2.append(row['gameSpeed2'])
  matchId2.append(row['matchId2']) 
  matchResult2.append(row['matchResult2'])
  matchType2.append(row['matchType2'])
  playTime2.append(row['playTime2'])
  startTime2.append(row['startTime2'])
  trackId2.append(row['trackId2'])
  teamId2.append(row['teamId2'])
  teamPlayers2.append(row['teamPlayers2'])
  p_accountNo2.append(row['p_accountNo2'])
  p_characterName2.append(row['p_characterName2'])
  p_character2.append(row['p_character2'])
  p_kart2.append(row['p_kart2'])
  p_license2.append(row['p_license2'])
  p_pet2.append(row['p_pet2'])
  p_flyingPet2.append(row['p_flyingPet2'])
  p_partsEngine2.append(row['p_partsEngine2'])
  p_partsHandle2.append(row['p_partsHandle2'])
  p_partsWheel2.append(row['p_partsWheel2'])
  p_partsKit2.append(row['p_partsKit2'])
  p_rankinggrade22.append(row['p_rankinggrade22'])
  p_matchRank2.append(row['p_matchRank2'])
  p_matchRetired2.append(row['p_matchRetired2'])
  p_matchWin2.append(row['p_matchWin2'])
  p_matchTime2.append(row['p_matchTime2'])
  

df = pd.DataFrame(data=zip(channelName2,endTime2,gameSpeed2,matchId2,matchResult2,matchType2,playTime2,startTime2,trackId2,teamId2,teamPlayers2,p_accountNo2,p_characterName2,p_character2,p_kart2,p_license2,p_pet2,p_flyingPet2,p_partsEngine2,p_partsHandle2,p_partsWheel2,p_partsKit2,p_rankinggrade22,p_matchRank2,p_matchRetired2,p_matchWin2,p_matchTime2),columns=['channelName','endTime','gameSpeed','matchId','matchResult','matchType','playTime','startTime','trackId','teamId','teamPlayers','p_accountNo','p_characterName','p_character','p_kart','p_license','p_pet','p_flyingPet','p_partsEngine','p_partsHandle','p_partsWheel','p_partsKit','p_rankinggrade2','p_matchRank','p_matchRetired','p_matchWin','p_matchTime'])
df.to_csv('/dbfs/FileStore/KartRider/2/3/2021_09_30_(1)_speed_matchinfo_df.csv')


# COMMAND ----------

#확인작업

import pandas as pd
pandas_df = pd.read_csv("/dbfs/FileStore/KartRider/2/3/2021_09_30_(1)_speed_matchinfo_df.csv", header='infer') 
print(len(pandas_df["matchId"].unique()))
pandas_df

# COMMAND ----------

#수정전 게임데이터 수집코드(참고용!!!)

import parmap
import multiprocessing
import requests
import time as ti
import pandas as pd

num_cores = multiprocessing.cpu_count()
manager = multiprocessing.Manager()
channelName2 = manager.list()
endTime2 = manager.list()
gameSpeed2 = manager.list()
matchId3 = manager.list()
matchResult2 = manager.list() # vs win... 
matchType2 = manager.list()
playTime2 = manager.list()
startTime2 = manager.list()
trackId2 = manager.list()
teamId2 = manager.list()
teamPlayers2 = manager.list()
p_accountNo2 = manager.list()
p_characterName2 = manager.list()
p_character2 = manager.list()
p_kart2 = manager.list()
p_license2 = manager.list()
p_pet2 = manager.list()
p_flyingPet2 = manager.list()
p_partsEngine2 = manager.list()
p_partsHandle2 = manager.list()
p_partsWheel2 = manager.list()
p_partsKit2 = manager.list()
p_rankinggrade22 = manager.list()
p_matchRank2 = manager.list()
p_matchRetired2 = manager.list()
p_matchWin2 = manager.list()
p_matchTime2 = manager.list()
ex_data = manager.list()
ex_x = manager.list()
tmpid = manager.list()

def a(data):
  try:
    for x in data:
      tmpid.append(x)
      ti.sleep(1)
      url1 = "https://api.nexon.co.kr/kart/v1.0/matches/{}".format(x)
      r = requests.get(url=url1, headers={'Authorization': my_token}).json()
      print(r)
      #tmpid.append(X)
#       channelName2.append(r['channelName'])
#       endTime2.append(r['endTime'])
#       gameSpeed2.append(r['gameSpeed'])
#       matchId2.append(r['matchId']) 
#       matchResult2.append(r['matchResult'])
#       matchType2.append(r['matchType'])
#       playTime2.append(r['playTime'])
#       startTime2.append(r['startTime'])
#       trackId2.append(r['trackId'])
      for i in range(0,2):
        for j in range(0, len(r['teams'][i]['players'])):
          channelName2.append(r['channelName'])
          endTime2.append(r['endTime'])
          gameSpeed2.append(r['gameSpeed'])
          matchId2.append(r['matchId'])
          matchId3.append(r['matchId']) 
          matchResult2.append(r['matchResult'])
          matchType2.append(r['matchType'])
          playTime2.append(r['playTime'])
          startTime2.append(r['startTime'])
          trackId2.append(r['trackId'])
          teamId2.append(r['teams'][i]['teamId'])
          teamPlayers2.append(len(r['teams'][i]['players']))
          p_accountNo2.append(r['teams'][i]['players'][j]['accountNo'])
          p_characterName2.append(r['teams'][i]['players'][j]['characterName'])
          p_character2.append(r['teams'][i]['players'][j]['character'])
          p_kart2.append(r['teams'][i]['players'][j]['kart'])
          p_license2.append(r['teams'][i]['players'][j]['license'])
          p_pet2.append(r['teams'][i]['players'][j]['pet'])
          p_flyingPet2.append(r['teams'][i]['players'][j]['flyingPet'])
          p_partsEngine2.append(r['teams'][i]['players'][j]['partsEngine'])
          p_partsHandle2.append(r['teams'][i]['players'][j]['partsHandle'])
          p_partsWheel2.append(r['teams'][i]['players'][j]['partsWheel'])
          p_partsKit2.append(r['teams'][i]['players'][j]['partsKit'])
          p_rankinggrade22.append(r['teams'][i]['players'][j]['rankinggrade2'])
          p_matchRank2.append(r['teams'][i]['players'][j]['matchRank'])
          p_matchRetired2.append(r['teams'][i]['players'][j]['matchRetired'])
          p_matchWin2.append(r['teams'][i]['players'][j]['matchWin'])
          p_matchTime2.append(r['teams'][i]['players'][j]['matchTime'])
  except:
    ex_data.append(data)
    ex_x.append(x)
      
      
    

      
            
    return matchId3
            
parmap.map(a, splited_timedata, pm_pbar=True, pm_processes=12)
df = pd.DataFrame(data=zip(channelName2,endTime2,gameSpeed2,matchId3,matchResult2,matchType2,playTime2,startTime2,trackId2,teamId2,teamPlayers2,p_accountNo2,p_characterName2,p_character2,p_kart2,p_license2,p_pet2,p_flyingPet2,p_partsEngine2,p_partsHandle2,p_partsWheel2,p_partsKit2,p_rankinggrade22,p_matchRank2,p_matchRetired2,p_matchWin2,p_matchTime2),columns=['channelName','endTime','gameSpeed','matchId','matchResult','matchType','playTime','startTime','trackId','teamId','teamPlayers','p_accountNo','p_characterName','p_character','p_kart','p_license','p_pet','p_flyingPet','p_partsEngine','p_partsHandle','p_partsWheel','p_partsKit','p_rankinggrade2','p_matchRank','p_matchRetired','p_matchWin','p_matchTime'])
df.to_csv('/dbfs/FileStore/KartRider/2021_08_07_speed_matchinfo_df.csv')

temp =[]
temp.extend(matchId3)
# difference b/w two lists
# error = list(set(matchid_list2) -set(Counter(matchId3).keys()))
# import json
# with open('/dbfs/FileStore/KartRider/2/3/08_07errorid.txt', 'w') as f:
#     f.write(json.dumps(error))
# with open('/dbfs/FileStore/KartRider/2/3/08_07errorid.txt', 'r') as f:
#     a = json.loads(f.read())