
import pymysql
import datetime
import configparser
import numpy as np
import time
import scipy.stats


##############################################################################################################################
##############################################################################################################################
# Basic Functions (quotes, referential)
##############################################################################################################################
##############################################################################################################################



def get_quotes(ticker, date):
    command = 'SELECT * FROM quotes where ticker = "%s" and dt = "%s" ' % (ticker , date)
    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return False
    
    output = []

    for item in result:
        output.append(item[2:])

    return output


def add_update_quote (ticker, dt, timestamp,qopen,high,low,close,volume):

    command = 'INSERT INTO quotes VALUES ("%s","%s","%s","%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE open ="%s", high ="%s" , low = "%s", close = "%s", volume = "%s" '\
         %  (ticker, dt, timestamp,qopen,high,low,close,volume,qopen,high,low,close,volume)
    try: 
        cur.execute(command)
    except pymysql.DataError as e :
        print (e)
        return False
    return True


def get_ticker_list(ETF_Flag = True):
    
    if ETF_Flag:
        command = 'SELECT DISTINCT ticker FROM quotes' 
    else :
        command = 'SELECT DISTINCT ticker FROM quotes where ticker not in (select ticker from static where industry = "ETF")' 

    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return False
    output =[]
    for item in result:
        output.append(item[0])

    return output

def get_date_list(ticker):
    command = 'SELECT DISTINCT dt FROM quotes where ticker = "%s"' % ticker 
    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return False
    output =[]
    for item in result:
        output.append(item[0])

    return output


def get_previous_day(ticker, date):
    command = 'SELECT DISTINCT dt FROM quotes where ticker = "%s"' % ticker 
    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return 'Error'
    output =[]
    previous_day = False
    for item in result:
        if item[0] == date :
            return(previous_day)
        previous_day = item[0]  

    return False


def get_next_day_feats(date):
    command = 'SELECT DISTINCT dt FROM feats1d where dt > "%s" order by dt limit 1' % date
    try: 
        count = cur.execute(command)
        result = cur.fetchall()

    except pymysql.DataError as e :
        print (e)
        return 'Error'

    return result[0][0]   

def get_previous_day_feats(date):
    command = 'SELECT DISTINCT dt FROM feats1d where dt < "%s" order by dt DESC limit 1' % date
    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return 'Error'

    return result[0][0]   


def get_date_list_feats(ticker = 'all'):

    if ticker == 'all':
        command = 'SELECT DISTINCT dt FROM feats1d order by dt' 
    else:
        command = 'SELECT DISTINCT dt FROM feats1d where ticker = "%s" order by dt' % ticker 
    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return False
    output =[]
    for item in result:
        output.append(item[0])

    return output


def get_previous_day_feats_ticker(ticker, date):
    command = 'SELECT DISTINCT dt FROM feats1d where ticker = "%s"' % ticker 
    try: 
        count = cur.execute(command)
        result = cur.fetchall()
    except pymysql.DataError as e :
        print (e)
        return 'Error'
    output =[]
    previous_day = False
    for item in result:
        if item[0] == date :
            return(previous_day)
        previous_day = item[0]  


    return False


def close_db():
    GHdb.close()

def commit_db():
    GHdb.commit()

def rollback_db():
    GHdb.rollback()




##########################################
##############  MAIN

GHdb = pymysql.connect('localhost',port=3306, user='root', passwd='', db='DB')
cur = GHdb.cursor()

