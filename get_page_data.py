from dataclasses import dataclass 
from lxml import html
import requests
import os
import sqlite3
import json
import time
from datetime import datetime


telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
db_token = os.environ.get("CLOUD_DB_AUTH_TOKEN")
db_end_point = os.environ.get("CLOUD_DB_END_POINT")

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
  

def processForOneSite__(data:ParseWebPage, cursor):
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
    page = requests.get(data.pageUrl, headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'})
    tree = html.fromstring(page.content)
    articleList = tree.xpath(data.listXpath)

    damoangException = False
    damoangUrl = "https://damoang.net"

    visitedUrls = []
    if(data.pageUrl.startswith(damoangUrl)):
      damoangException = True


    for article in articleList:
    
      title = None
      link = None
      titleList = article.xpath(data.titleXpath)
      if(titleList != None and len(titleList) > 0):
        title = titleList[0].strip()

      linkList = article.xpath(data.linkXpath)
      if(linkList != None and len(linkList) > 0):
        link = linkList[0]
        if(damoangException and link.startswith("/free/")):
          link = f'{damoangUrl}{link}'

      print(f'하나의 글 : {article} / {article.items()} /  타이틀 xpaht : {data.titleXpath} 타이틀 {title}/ link xpath: {data.linkXpath} 링크 {link}')

      # if(title != None and link != None and link.startswith("http") == True):
        
      #   cur.execute(f'SELECT count(*) FROM {data.tableName} where url=\'{link}\'')
      #   count = cur.fetchone()
      #   if count[0] > 0 :
      #     print(f'이미 있어서 보내지 앖음. {title} / {link} / {count}')
      #   else: 
      #     print(f'전달 {title} / {link} / {count}')
      #     visitedUrls.append((link,))
      #     send_telegram(data.tChannelId, f'{title}\n{link}')
        
      #   # data.tChannelId

    # if len(visitedUrls) > 0:
    #   print('값을 추가 해주자. ', visitedUrls)
    #   cursor.executemany(f"INSERT INTO {data.tableName} VALUES(?, date());", visitedUrls)

    # //*[@id="main-wrap"]/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/ul[1]/li[1]
    # //*[@id="main-wrap"]/div/div/div[1]/div/div[2]/div[1]/div[2]/ul/li
    # //*[@id="main-wrap"]/div/div/div[1]/div/div[2]/div[1]/div[2]/ul/li
    # //*[@id="main-wrap"]/div/div/div[1]/div/div[2]/div[1]/div[2]/ul/li[1]

    return (True, len(visitedUrls) > 0)
  except Exception as e:    
    print('예외가 발생했습니다.', e)
    return (False, False)


def checkVisited(visitedList, url):
  for visitedUrl in visitedList:
    if(visitedUrl["url"] == url):
      return True
  return False


def processForOneSite(siteDataDict):
  print(f'{siteDataDict["name"]} 사이트 검색 시작 ')
  try:
    # 이미 확인한 글들 가져오자. 
    checkedList = [];
    checkedListReqeustResult = dbQuery({'query':'query pasing_websites_visitedCheck { pasing_websites_visitedCheck ( limit:500  filter: {websiteid: {eq:'+str(siteDataDict["rowid"])+'}} order_by: [{checkDate:DESC}]) {rowid url checkDate}}'})

    if(checkedListReqeustResult != None):
      print('결과 ')
      print(checkedListReqeustResult)
      checkedList = checkedListReqeustResult["data"]["pasing_websites_visitedCheck"]
      print(checkedList)


    # 100개가 넘는 결과가 나온다면 200개까지 결과를 줄여주자. 
    # checkDate 순으로 역순으로 오기 때문에 200개 이후의 결과를 지워주면 된다. 
    if(len(checkedList)>200):
      deleteTargetList = checkedList[200:]
      checkedList = checkedList[:200]
      # 어떤 이유인지 한번에 지우는게 안된다. 하나씩??? 지워야 한다. 
      for deleteTarget in deleteTargetList:
        print(dbQuery({'query':'mutation delete_pasing_websites_visitedCheck { delete_pasing_websites_visitedCheck(filter: {rowid: {eq: '+str(deleteTarget["rowid"])+'}}) { affected_rows } }'}))




    page = requests.get(siteDataDict["pageUrl"], headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'})
    tree = html.fromstring(page.content)
    articleList = tree.xpath(siteDataDict["listXpath"])

    damoangException = False
    damoangUrl = "https://damoang.net"
    if(siteDataDict["pageUrl"].startswith(damoangUrl)):
      damoangException = True

    sendUrls = ""
    today = datetime.today().strftime("%Y%m%d")

    for article in articleList:
      title = None
      link = None
      titleList = article.xpath(siteDataDict["titleXpath"])
      if(titleList != None and len(titleList) > 0):
        title = titleList[0].strip()

      linkList = article.xpath(siteDataDict["linkXpath"])
      if(linkList != None and len(linkList) > 0):
        link = linkList[0]
        if(damoangException and link.startswith("/free/")):
          link = f'{damoangUrl}{link}'

      print(f'하나의 글 : {article} / {article.items()} /  타이틀 xpaht : {siteDataDict["titleXpath"]} 타이틀 {title}/ link xpath: {siteDataDict["linkXpath"]} 링크 {link}')

      if(title != None and link != None and link.startswith("http") == True):
        # 이미 보낸 글인지 확인한다. 
        if checkVisited(checkedList, link):
          print('이미 보낸글이라서 보내지 않습니다.')
        else:
          print('여기서는 보내보자')
          sendUrls += '{websiteid:'+str(siteDataDict["rowid"])+', url:"'+link+'", checkDate:"'+today+'"},'
          send_telegram(siteDataDict["telegramChannel"], f'{title}\n{link}')

    if(len(sendUrls) > 0):
      # dump = json.dumps(sendUrls)
      dump = '['+sendUrls[0:-1]+']'
      # print(dump)
      print("============================================ 이후 쿼리출려 ")
      print({'query':'mutation pasing_websites_visitedCheck { insert_pasing_websites_visitedCheck ( objects:'+dump+') {affected_rows returning {rowid}} }'})
      dbQuery({'query':'mutation pasing_websites_visitedCheck { insert_pasing_websites_visitedCheck ( objects:'+dump+') {affected_rows returning {rowid}} }'})

  except Exception as e:
    print('예외가 발생했습니다.', e)
  

def dbQuery(queryData):
  r = requests.post(db_end_point, data=json.dumps(queryData), headers={'Content-Type':'application/json', 'Authorization':db_token})
  if(r.status_code == 200):
    return json.loads(r.text)
  else:
    print(f'디비 요청 에러 {r.status_code} / {r.text}')
    return None



def process():
  #전체 검사해야 할 웹페이지 메타 데이터를 가져온다. 
  webSites = dbQuery({'query':'query parsing_websites{parsing_websites {rowid name pageUrl listXpath linkXpath titleXpath telegramChannel}}'})
  if(webSites == None):
    # 오류구만 종료한다. 
    return 
    

  for oneSite in webSites["data"]["parsing_websites"]:
    print(oneSite["name"])
    processForOneSite(oneSite)




process()
# 전역으로 연결할 sqlite3 연결을 생성.
# dbCon = sqlite3.connect('./parsing.db')
# cur = dbCon.cursor()
# cur.execute('SELECT * from webSites')
# webSiteList = cur.fetchall()

# needCommit = False

# commitCount = 0

# for webSite in webSiteList:
#   print(f'파싱 시작! 사이트 : {webSite}')
#   site = ParseWebPage()
#   site.pageUrl = webSite[1]
#   site.listXpath = webSite[2]
#   site.titleXpath = webSite[4]
#   site.linkXpath = webSite[3]
#   site.tableName = webSite[6]
#   site.tChannelId = webSite[5]
#   result, oneSiteCommit = process(site, cur)

#   print(f'파싱 완료! 결과는 : {result} / {oneSiteCommit}')
#   #cur.execute(f"delete from {site.tableName} where checkDate < date('now','-1 day');")
#   needCommit = needCommit or oneSiteCommit


# if needCommit: 
#   dbCon.commit()
#   commitCount = commitCount + 1

# dbCon.close()


# logTime = time.strftime('%Y.%m.%d - %H:%M:%S')
# f = open('./lastCheckTime.log','w')
# f.write(logTime)
# f.close()


os.system(f'echo \'list_count=0\' >> $GITHUB_OUTPUT')
