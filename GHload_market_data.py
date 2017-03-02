
import GHdb
import xlrd
import urllib.request as url
import time
import numpy as np
import subprocess
import datetime
import sys
import om_global
from datetime import datetime
from datetime import timedelta
import csv



############################################
##############  FUNCTIONS

def get_google(Sec_Name, NbDate):


    Output = []

    # collect data on the date


    base_url = 'http://www.google.com/finance/getprices?i=60&p='+str(NbDate)+'d&f=d,o,h,l,c,v&df=cpct&q='+Sec_Name
    response = url.urlopen(base_url)

    cr=response.read().decode('utf-8')
    list_quotes = cr.split('\n')

    start = 0
    for i in range(0, len(list_quotes)-1):
        current_line = list_quotes[i].split(',')
        if current_line[0][:3] == 'a14':
            current_date = int(current_line[0][1:]) - 60
            current_TimeStamp = current_date
            start += 1
        elif start !=0 and current_line[0][:4]!='TIME':
            current_TimeStamp = current_date + int(current_line[0]) * 60
            
        if start != 0 and start <= NbDate and current_line[0][:4]!='TIME':
            #print(current_TimeStamp)
            Temp = datetime.fromtimestamp(current_TimeStamp).strftime('%Y-%m-%d,%H:%M:%S').split(',')
            Temp.extend([current_line[4],current_line[2], current_line[3], current_line[1], current_line[5]])
            Output.append(Temp)

    return Output



def GetSPYComponents():

    base_url = 'https://www.spdrs.com/site-content/xls/SPY_All_Holdings.xls?fund=SPY&docname=All+Holdings&onyx_code1=1286&onyx_code2=1700'

    url.urlretrieve(base_url, ‘~/SPY_All_Holdings.xls’)
    

    wb = xlrd.open_workbook('~/SPY_All_Holdings.xls')
    sh = wb.sheet_by_name('SPY_All_Holdings')

    output = [['na', time.strftime("%m/%d/%Y"), time.strftime("%m/%d/%Y"), 'na', 'na']]
    
    for rownum in range(3, sh.nrows):
        output.append(sh.row_values(rownum))
        

    output = np.array(output)

    output = output[:,[1,2]]

    for i in range (0, len(output)):
         if output[i][0]=='' :
             Length = i
             break

    stock_list = []
    dirty_stock_list = output[:,0][2:]
    for item in dirty_stock_list:
        if len(item)>0 and 'CASH_' not in item:
            stock_list.append(item)

    return stock_list


############################################
##############  MAIN

print ('Start')

args = eval(str(sys.argv))

nb_days = 1
SPY_List = []


if 'NOSPY' in args: 
    if len(args)==3: nb_days = int(args[1])

else:
    if len(args)==2: nb_days = int(args[1])
    SPY_List = GetSPYComponents()

print ("Nb Days :", nb_days)

current_db_list = GHdb.get_ticker_list()

stock_list= list(set(current_db_list+SPY_List))

start_dt = '2100-01-01'

print (len(SPY_List), len(current_db_list), len(stock_list))
count_q = 0
for ticker in stock_list:

    print (ticker)

    quotes = get_google(ticker, nb_days)

    if len(quotes) != 0 :
        for quote in quotes:
            if '20' in quote[0] :
                if start_dt > quote[0]: start_dt = quote[0]
                GHdb.add_update_quote(ticker, quote[0], quote[1],quote[2],quote[3],quote[4], quote[5],quote[6] )
                count_q += 1

    GHdb.commit_db()

print ('%s quotes added' % count_q)
print ('End Loading')

GHdb.close_db()








