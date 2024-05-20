from dataclasses import dataclass 
from lxml import html
import requests
import sys

@dataclass 
class ParseWebPage:
  pageUrl: str = None
  listXpath: str = None
  linkXpath: str = None
  titleXpath: str = None
  tChannelId: str = None
  tableName: str = None


def process(data:ParseWebPage):
  if(
    data.pageUrl == None or 
    data.linkXpath == None or
    data.listXpath == None or
    data.titleXpath == None
    ):
    print('파싱할 데이터가 없음')
    return (False, False)
  try:
    print(f'파싱할 url {data.pageUrl}')

    page = requests.get(data.pageUrl, headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'})
    tree = html.fromstring(page.content)
    articleList = tree.xpath(data.listXpath)

    visitedUrls = []

    print(page)

    print('===================================')

    for article in articleList:

      print(article)

      title = None
      link = None
      print(f'하나의 글 : {article} / {article.items()} /  타이틀 xpaht : {data.titleXpath} / link xpath: {data.linkXpath}')
      titleList = article.xpath(data.titleXpath)
      print('1111')
      if(titleList != None and len(titleList) > 0):
        title = titleList[0].strip()

      print(title)

      print('22220')
      print(data.linkXpath)
      linkList = article.xpath(data.linkXpath)
      print('222211')
      if(linkList != None and len(linkList) > 0):
        link = linkList[0]

      print('3333')
      print(f'{titleList} / {linkList}')

      if(title != None and link != None):
        print(f'파싱!. {title} / {link}')
    
  except Exception as e:    
    print('예외가 발생했습니다.', e)
    return (False, False)



if __name__ == '__main__' :

  d = ParseWebPage()
  d.pageUrl = sys.argv[1]
  d.listXpath = sys.argv[2]
  d.linkXpath = sys.argv[3]
  d.titleXpath = sys.argv[4]
  process(d)