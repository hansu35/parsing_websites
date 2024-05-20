from dataclasses import dataclass 
from lxml import html
import requests
import os
import sqlite3
import json
import time


telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")

@dataclass 
class ParseWebPage:
  pageUrl: str = None
  listXpath: str = None
  linkXpath: str = None
  titleXpath: str = None
  tChannelId: str = None
  tableName: str = None

## 텔레그램 메세지 전달. 
def send_telegram(chat_id, massage, ):
  datas = {
  'chat_id':chat_id,
  'text':massage 
  }
  print(f'텔레그램 발송 데이터 {datas}')
  try:
    telegram_url = f'https://api.telegram.org/bot{telegram_bot_token}/sendMessage'
    r = requests.post(telegram_url, data=json.dumps(datas), headers={'Content-Type':'application/json'})

    if(r.status_code != 200):
      print('텔레그램 발송 실패.')
      print(r.text)
  except Exception as e:    
    print('텔레그램 전송에 예외가 발생했습니다.', e)
    return (False, False)
  

def process(data:ParseWebPage, cursor):
  if(
    data.pageUrl == None or 
    data.linkXpath == None or
    data.listXpath == None or
    data.titleXpath == None
    ):
    print('파싱할 데이터가 없음')
    return (False, False)
  if(data.tableName == None or data.tChannelId == None or cursor == None):
    print('검사하고 전달할 자료가 없음')
    return (False, False)

  try:
    page = requests.get(data.pageUrl)
    tree = html.fromstring(page.content)
    articleList = tree.xpath(data.listXpath)

    visitedUrls = []

    for article in articleList:

      title = None
      link = None
      titleList = article.xpath(data.titleXpath)
      if(titleList != None and len(titleList) > 0):
        title = titleList[0].strip()

      linkList = article.xpath(data.linkXpath)
      if(linkList != None and len(linkList) > 0):
        link = linkList[0]

      if(title != None and link != None):
        
        cur.execute(f'SELECT count(*) FROM {data.tableName} where url=\'{link}\'')
        count = cur.fetchone()
        if count[0] > 0 :
          print(f'이미 있어서 보내지 앖음. {title} / {link} / {count}')
        else: 
          print(f'전달 {title} / {link} / {count}')
          visitedUrls.append((link,))
          send_telegram(data.tChannelId, f'{title}\n{link}')
        
        # data.tChannelId

    if len(visitedUrls) > 0:
      print('값을 추가 해주자. ', visitedUrls)
      cursor.executemany(f"INSERT INTO {data.tableName} VALUES(?, date());", visitedUrls)

    return (True, len(visitedUrls) > 0)
  except Exception as e:    
    print('예외가 발생했습니다.', e)
    return (False, False)




# 전역으로 연결할 sqlite3 연결을 생성.
dbCon = sqlite3.connect('./parsing.db')
cur = dbCon.cursor()
cur.execute('SELECT * from webSites')
webSiteList = cur.fetchall()

needCommit = False

commitCount = 0

for webSite in webSiteList:
  print(f'파싱 시작! 사이트 : {webSite}')
  site = ParseWebPage()
  site.pageUrl = webSite[1]
  site.listXpath = webSite[2]
  site.titleXpath = webSite[4]
  site.linkXpath = webSite[3]
  site.tableName = webSite[6]
  site.tChannelId = webSite[5]
  result, oneSiteCommit = process(site, cur)

  print(f'파싱 완료! 결과는 : {result} / {oneSiteCommit}')
  needCommit = needCommit or oneSiteCommit


if needCommit: 
  dbCon.commit()
  commitCount = commitCount + 1

dbCon.close()


logTime = time.strftime('%Y.%m.%d - %H:%M:%S')
f = open('./lastCheckTime.log','w')
f.write(logTime)
f.close()


os.system(f'echo \'list_count={commitCount}\' >> $GITHUB_OUTPUT')