# stock_mysql
Creates a mySQL database and loads stock returns data from google finance on a daily basis.

The stock list is pulled from the official S&P 500 components plus the stocks already present in the database.

STEPS :
1. install mysql and pymysql
2. execute "python3 GHcreate_tables.py" to create the quote table
3. execute "python3 GHload_market_data.py " + the number of historical days you want to load from google (max 10 days / 2 weeks)
