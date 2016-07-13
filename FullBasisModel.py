#Transco Zone 6 NY Basis futures settlements

from bs4 import BeautifulSoup
import psycopg2
import urllib2
import datetime
import pprint
from os import sys

#basis prices seem to be posted the next morning on CME, so use todays date -1

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
stamp = yesterday.strftime('%Y-%m-%d')

days=['2016-01-09', '2016-01-10', '2016-01-16', '2016-01-17', '2016-01-23', '2016-01-24', '2016-01-30', '2016-01-31', '2016-02-06', '2016-02-07', '2016-02-13', '2016-02-14', '2016-02-20', '2016-02-21', '2016-02-27', '2016-02-28',
      '2016-03-05', '2016-03-06', '2016-03-12', '2016-03-13', '2016-03-19', '2016-03-20', '2016-03-26', '2016-03-27', '2016-04-02', '2016-04-03', '2016-04-09', '2016-04-10', '2016-04-16', '2016-04-17', '2016-04-23', '2016-04-24',
      '2016-04-30', '2016-05-01', '2016-05-07', '2016-05-08', '2016-05-14', '2016-05-15', '2016-05-21', '2016-05-22', '2016-05-28', '2016-05-29', '2016-06-04', '2016-06-05', '2016-06-11', '2016-06-12', '2016-06-18', '2016-06-19',
      '2016-06-25', '2016-06-26', '2016-07-02', '2016-07-03', '2016-07-09', '2016-07-10', '2016-07-16', '2016-07-17', '2016-07-23', '2016-07-24', '2016-07-30', '2016-07-31', '2016-08-06', '2016-08-07', '2016-08-13', '2016-08-14',
      '2016-08-20', '2016-08-21', '2016-08-27', '2016-08-28', '2016-09-03', '2016-09-04', '2016-09-10', '2016-09-11', '2016-09-17', '2016-09-18', '2016-09-24', '2016-09-25', '2016-10-01', '2016-10-02', '2016-10-08', '2016-10-09',
      '2016-10-15', '2016-10-16', '2016-10-22', '2016-10-23', '2016-10-29', '2016-10-30', '2016-11-05', '2016-11-06', '2016-11-12', '2016-11-13', '2016-11-19', '2016-11-20', '2016-11-26', '2016-11-27', '2016-12-03', '2016-12-04',
      '2016-12-10', '2016-12-11', '2016-12-17', '2016-12-18', '2016-12-24', '2016-12-25', '2016-12-31', '2016-05-30', '2016-07-04', '2016-09-05', '2016-10-10', '2016-11-25', '2016-12-26']
if stamp in days:
    sys.exit(0)

myDb = 'web'

try:
    conn = psycopg2.connect(database=myDb, user='', password='', host='127.0.0.1', port='5432')
    print 'connection to databse %s successful' %(myDb)
except:
    print 'could not open database %s' %(myDb)

#start web scrape

basisPage = urllib2.Request("http://www.cmegroup.com/trading/energy/natural-gas/transco-zone-6-new-york-natural-gas-basis-swap-futures-platts-iferc_quotes_settlements_futures.html", headers = {'User-Agent': 'Mozilla/5.0'})
cmeHtml = urllib2.urlopen(basisPage).read()
soup = BeautifulSoup(cmeHtml, "html.parser")



data = []
table = soup.find('table', attrs ={'class': 'cmeTable'})
table_body = table.find('tbody')

rows = table_body.find_all('tr')
head = table_body.find_all('th')
for row in rows:
    cols = row.find_all('td')
    cols = cols[5]
    #cols = [ele.text.strip() for ele in cols]
    month = [ele.text.strip() for ele in head]
    data.append([ele for ele in cols if ele]) # Get rid of empty values

values = ({"month":month[0], "settle":data[0], "date": stamp},
            {"month":month[1], "settle":data[1], "date": stamp}, 
            {"month":month[3], "settle":data[3], "date": stamp},
            {"month":month[2], "settle":data[2], "date": stamp},
            {"month":month[4], "settle":data[4], "date": stamp},
            {"month":month[5], "settle":data[5], "date": stamp},
            {"month":month[6], "settle":data[6], "date": stamp},
            {"month":month[7], "settle":data[7], "date": stamp},
            {"month":month[8], "settle":data[8], "date": stamp},
            {"month":month[9], "settle":data[9], "date": stamp}, 
            {"month":month[10], "settle":data[10], "date": stamp},
            {"month":month[11], "settle":data[11], "date": stamp},
            {"month":month[12], "settle":data[12], "date": stamp},
            {"month":month[13], "settle":data[13], "date": stamp},
            {"month":month[14], "settle":data[14], "date": stamp},
            {"month":month[15], "settle":data[15], "date": stamp})

print 'Transco Zone 6 NY Basis futures settlements \n'
try:
    cur = conn.cursor()
    for i in values:
        cur.executemany("INSERT INTO transcony (month, settle, date) VALUES (%(month)s, %(settle)s, %(date)s)", values)
        print 'inserting row', i
    conn.commit()
    print '-=-'*25
    
except psycopg2.Error as e:
    print '\n' +'Error code:', e.pgcode, '\n', e.pgerror
    
#-------------------------------------------------------------------------------

#Transco Non-NY Basis Futures
basisPage = urllib2.Request("http://www.cmegroup.com/trading/energy/natural-gas/transco-zone-6-non-ny-platts-iferc-basis-swap_quotes_settlements_futures.html", headers = {'User-Agent': 'Mozilla/5.0'})
cmeHtml = urllib2.urlopen(basisPage).read()
soup = BeautifulSoup(cmeHtml, "html.parser")


data = []
table = soup.find('table', attrs ={'class': 'cmeTable'})
table_body = table.find('tbody')

rows = table_body.find_all('tr')
for row in rows:
    head = table_body.find_all('th')
    cols = row.find_all('td')
    cols = cols[5]
    #cols = [ele.text.strip() for ele in cols]
    month = [ele.text.strip() for ele in head]
    data.append([ele for ele in cols if ele]) # Get rid of empty values



values =   ({"month":month[0], "settle":data[0], "date": stamp},
            {"month":month[1], "settle":data[1], "date": stamp}, 
            {"month":month[2], "settle":data[2], "date": stamp},
            {"month":month[3], "settle":data[3], "date": stamp},
            {"month":month[4], "settle":data[4], "date": stamp},
            {"month":month[5], "settle":data[5], "date": stamp},
            {"month":month[6], "settle":data[6], "date": stamp},
            {"month":month[7], "settle":data[7], "date": stamp},
            {"month":month[8], "settle":data[8], "date": stamp},
            {"month":month[9], "settle":data[9], "date": stamp}, 
            {"month":month[10], "settle":data[10], "date": stamp},
            {"month":month[11], "settle":data[11], "date": stamp},
            {"month":month[12], "settle":data[12], "date": stamp},
            {"month":month[13], "settle":data[13], "date": stamp},
            {"month":month[14], "settle":data[14], "date": stamp},
            {"month":month[15], "settle":data[15], "date": stamp},
            {"month":month[16], "settle":data[16], "date": stamp},
            {"month":month[17], "settle":data[17], "date": stamp}, 
            {"month":month[18], "settle":data[18], "date": stamp},
            {"month":month[19], "settle":data[19], "date": stamp},
            {"month":month[20], "settle":data[20], "date": stamp},
            {"month":month[21], "settle":data[21], "date": stamp},
            {"month":month[22], "settle":data[22], "date": stamp})

 
print 'Transco Non-NY Basis Futures \n'
try:
    cur = conn.cursor()
    cur.executemany("INSERT INTO transconny (month, settle, tradedate) VALUES (%(month)s, %(settle)s, %(date)s)", values)
    for i in values:
        print 'inserting row', i
    conn.commit()
    print '-=-'*25
    
except psycopg2.Error as e:
    print '\n' +'Error code:', e.pgcode, '\n', e.pgerror

#-------------------------------------------------------------------------------
#Texas M3 Basis Futures Settlements

basisPage = urllib2.Request("http://www.cmegroup.com/trading/energy/natural-gas/texas-eastern-zone-m-3-natural-gas-basis-swap-futures-platts-iferc_quotes_settlements_futures.html", headers = {'User-Agent': 'Mozilla/5.0'})
cmeHtml = urllib2.urlopen(basisPage).read()
soup = BeautifulSoup(cmeHtml, "html.parser")


data = []
table = soup.find('table', attrs ={'class': 'cmeTable'})
table_body = table.find('tbody')

rows = table_body.find_all('tr')
head = table_body.find_all('th')
for row in rows:
    cols = row.find_all('td')
    cols = cols[5]
    #cols = [ele.text.strip() for ele in cols]
    month = [ele.text.strip() for ele in head]
    data.append([ele for ele in cols if ele]) # Get rid of empty values

values = (  {"month":month[0], "settle":data[0], "date": stamp},
            {"month":month[1], "settle":data[1], "date": stamp}, 
            {"month":month[2], "settle":data[2], "date": stamp},
            {"month":month[3], "settle":data[3], "date": stamp},
            {"month":month[4], "settle":data[4], "date": stamp},
            {"month":month[5], "settle":data[5], "date": stamp},
            {"month":month[6], "settle":data[6], "date": stamp},
            {"month":month[7], "settle":data[7], "date": stamp},
            {"month":month[8], "settle":data[8], "date": stamp},
            {"month":month[9], "settle":data[9], "date": stamp}, 
            {"month":month[10], "settle":data[10], "date": stamp},
            {"month":month[11], "settle":data[11], "date": stamp},
            {"month":month[12], "settle":data[12], "date": stamp},
            {"month":month[13], "settle":data[13], "date": stamp},
            {"month":month[14], "settle":data[14], "date": stamp},
            {"month":month[15], "settle":data[15], "date": stamp},
            {"month":month[16], "settle":data[16], "date": stamp},
            {"month":month[17], "settle":data[17], "date": stamp}, 
            {"month":month[18], "settle":data[18], "date": stamp},
            {"month":month[19], "settle":data[19], "date": stamp},
            {"month":month[20], "settle":data[20], "date": stamp},
            {"month":month[21], "settle":data[21], "date": stamp},
            {"month":month[22], "settle":data[22], "date": stamp},
            {"month":month[23], "settle":data[23], "date": stamp},
            {"month":month[24], "settle":data[24], "date": stamp})

print 'Texas M3 Basis Futures Settlements \n'
try:
    cur = conn.cursor()
    cur.executemany("INSERT INTO texasm3 (month, settle, tradedate) VALUES (%(month)s, %(settle)s, %(date)s)", values)
    for i in values:
        print 'inserting row', i
    conn.commit()
    print '-=-'*25
    
except psycopg2.Error as e:
    print '\n' +'Error code:', e.pgcode, '\n', e.pgerror

#-------------------------------------------------------------------------------
#Algonquin City Gates Basis Futures Settlements

basisPage = urllib2.Request("http://www.cmegroup.com/trading/energy/natural-gas/algonquin-citygates-natural-gas-basis-futures_quotes_settlements_futures.html", headers = {'User-Agent': 'Mozilla/5.0'})
cmeHtml = urllib2.urlopen(basisPage).read()
soup = BeautifulSoup(cmeHtml, "html.parser")


data = []
table = soup.find('table', attrs ={'class': 'cmeTable'})
table_body = table.find('tbody')

rows = table_body.find_all('tr')
head = table_body.find_all('th')
for row in rows:
    cols = row.find_all('td')
    cols = cols[5]
    #cols = [ele.text.strip() for ele in cols]
    month = [ele.text.strip() for ele in head]
    data.append([ele for ele in cols if ele]) # Get rid of empty values



values = (  {"month":month[0], "settle":data[0], "date": stamp},
            {"month":month[1], "settle":data[1], "date": stamp}, 
            {"month":month[2], "settle":data[2], "date": stamp},
            {"month":month[3], "settle":data[3], "date": stamp},
            {"month":month[4], "settle":data[4], "date": stamp},
            {"month":month[5], "settle":data[5], "date": stamp},
            {"month":month[6], "settle":data[6], "date": stamp},
            {"month":month[7], "settle":data[7], "date": stamp},
            {"month":month[8], "settle":data[8], "date": stamp},
            {"month":month[9], "settle":data[9], "date": stamp}, 
            {"month":month[10], "settle":data[10], "date": stamp},
            {"month":month[11], "settle":data[11], "date": stamp},
            {"month":month[12], "settle":data[12], "date": stamp},
            {"month":month[13], "settle":data[13], "date": stamp},
            {"month":month[14], "settle":data[14], "date": stamp},
            {"month":month[15], "settle":data[15], "date": stamp},
            {"month":month[16], "settle":data[16], "date": stamp},
            {"month":month[17], "settle":data[17], "date": stamp})

print 'Algonquin City Gates Basis Futures Settlements \n'

try:
    cur = conn.cursor()
    cur.executemany("INSERT INTO algonquin (month, settle, tradedate) VALUES (%(month)s, %(settle)s, %(date)s)", values)
    for i in values:
        print 'inserting row', i
    conn.commit()
    print '-=-'*25
    
except psycopg2.Error as e:
    print '\n' +'Error code:', e.pgcode, '\n', e.pgerror
      
finally:
    if conn:
        conn.close()
   
