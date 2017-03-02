#!/usr/bin/env python

import pymysql

db = pymysql.connect('local_host',port=3306, user='root', passwd='', db='DB')


quote_table = "CREATE TABLE quotes  (ticker VARCHAR(8), dt DATE, timestamp TIME, open VARCHAR(10), high VARCHAR(10), low VARCHAR(10), close VARCHAR(10), volume INT, PRIMARY KEY(ticker,dt,timestamp))"


cur = om_db.cursor()

cur.execute(quote_table)

db.commit()
db.close()

print ('executed')
