#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import pandas.io.sql as psql
import MySQLdb as mdb

# Obtain a database connection to the MySQL instance
db_host = '192.168.1.106'
db_user = 'songjiguo'
db_pass = 'kevinandy1A!'
db_name = 'practice'
con = mdb.connect(db_host, db_user, db_pass, db_name)

# Select all of the historic Google adjusted close data
sql = """SELECT dp.price_date, dp.adj_close_price
         FROM symbol AS sym
         INNER JOIN daily_price AS dp
         ON dp.symbol_id = sym.id
         WHERE sym.ticker = 'GOOG'
         ORDER BY dp.price_date ASC;"""

# Create a pandas dataframe from the SQL query
goog = psql.read_sql_query(sql, con=con, index_col='price_date')    

# Output the dataframe tail
print (goog.tail(n=10))  # default n=5

con.close()