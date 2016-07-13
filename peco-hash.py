#PECO -scrape tariff hyperlink from current tariff page, and hash it

from bs4 import BeautifulSoup
import hashlib
import urllib2



url='https://www.peco.com/CustomerService/RatesandPricing/RateInformation/Pages/CurrentElectric.aspx'
site=urllib2.Request(url)
subHtml=urllib2.urlopen(site).read()
soup=BeautifulSoup(subHtml,'html.parser')

#find div section where link is
ref = soup.find('div', attrs ={'id': 'ctl00_PlaceHolderMain_CenterCenterBodyRichHtmlField1__ControlWrapper_RichHtmlField'})

#isolate <a> tags
link=ref.find_all('a')

#find exact tariff link
target=link[0]['href']

#unicode text of effective tariff date - change to string so we can hash
text=str(link[0].getText().encode("utf-8",'ignore'))

#hash effective date, hyperlink to pdf
h1=hashlib.md5(target).hexdigest()
h2=hashlib.md5(text).hexdigest()

print 'hashing hyperlink:','\n' , target, '\n', 'to md5 hash:', h1, '\n'
print 'hashing text:' , text, '\n','to md5 hash:', h2


