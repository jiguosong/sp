#!/usr/bin/python
import MySQLdb
import datetime
from lxml import html
import requests
from math import ceil

def init_db():
    #db_name = "securities_master"
    db_name = "practice"
    db = MySQLdb.connect(host="192.168.1.106",    # your host, usually localhost
                         user="songjiguo",         # your username
                         passwd="kevinandy1A!",  # your password
                         db="%s"%(db_name))        # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()

    create_table = '''
    SET sql_notes = 0;
    CREATE TABLE IF NOT EXISTS `exchange` (
      `id` int NOT NULL AUTO_INCREMENT,
      `abbrev` varchar(32) NOT NULL,
      `name` varchar(255) NOT NULL,
      `city` varchar(255) NULL,
      `country` varchar(255) NULL,
      `currency` varchar(64) NULL,
      `timezone_offset` time NULL,
      `created_date` datetime NOT NULL,
      `last_updated_date` datetime NOT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    SET sql_notes = 1;
    '''

    cur.execute(create_table)

    create_table = '''
    SET sql_notes = 0;
    CREATE TABLE IF NOT EXISTS `data_vendor` (
      `id` int NOT NULL AUTO_INCREMENT,
      `name` varchar(64) NOT NULL,
      `website_url` varchar(255) NULL,
      `support_email` varchar(255) NULL,
      `created_date` datetime NOT NULL,
      `last_updated_date` datetime NOT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    SET sql_notes = 1;
    '''
    cur.execute(create_table)

    create_table = '''
    SET sql_notes = 0;
    CREATE TABLE IF NOT EXISTS `symbol` (
      `id` int NOT NULL AUTO_INCREMENT,
      `exchange_id` int NULL,
      `ticker` varchar(32) NOT NULL,
      `instrument` varchar(64) NOT NULL,
      `name` varchar(255) NULL,
      `sector` varchar(255) NULL,
      `currency` varchar(32) NULL,
      `created_date` datetime NOT NULL,
      `last_updated_date` datetime NOT NULL,
      PRIMARY KEY (`id`),
      KEY `index_exchange_id` (`exchange_id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    SET sql_notes = 1;
    '''
    cur.execute(create_table)

    create_table = '''
    SET sql_notes = 0;
    CREATE TABLE IF NOT EXISTS `daily_price` (
      `id` int NOT NULL AUTO_INCREMENT,
      `data_vendor_id` int NOT NULL,
      `symbol_id` int NOT NULL,
      `price_date` datetime NOT NULL,
      `created_date` datetime NOT NULL,
      `last_updated_date` datetime NOT NULL,
      `open_price` decimal(19,4) NULL,
      `high_price` decimal(19,4) NULL,
      `low_price` decimal(19,4) NULL,
      `close_price` decimal(19,4) NULL,
      `adj_close_price` decimal(19,4) NULL,
      `volume` bigint NULL,
      PRIMARY KEY (`id`),
      KEY `index_data_vendor_id` (`data_vendor_id`),
      KEY `index_synbol_id` (`symbol_id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    SET sql_notes = 1;
    '''
    cur.execute(create_table)

    #==============================================================================
    #
    # cur.execute("SHOW DATABASES")
    # res = cur.fetchall()
    # print (res)
    #
    cur.execute("SHOW TABLES in %s" %(db_name))
    res = cur.fetchall()
    print (res)
    #==============================================================================

    # Use all the SQL you like
    #cur.execute("SELECT * FROM symbol")

    # print all the first cell of all the rows
    #for row in cur.fetchall():
    #    print (row[0])

    return db

def insert_snp500_symbols(db, symbols):
    """Insert the S&P500 symbols into the MySQL database."""

    # Create the insert strings
    column_str = "ticker, instrument, name, sector, currency, created_date, last_updated_date"
    insert_str = ("%s, " * 7)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    print (final_str, len(symbols))

    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    with db:
        cur = db.cursor()
        # This line avoids the MySQL MAX_PACKET_SIZE
        # Although of course it could be set larger!
        for i in range(0, int(ceil(len(symbols) / 100.0))):
            cur.executemany(final_str, symbols[i*100:(i+1)*100-1])

def obtain_parse_wiki_snp500():
    """Download and parse the Wikipedia list of S&P500
    constituents using requests and libxml.

    Returns a list of tuples for to add to MySQL."""

    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()
    url = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    resp = requests.get(url)
    page = html.fromstring(resp.text)
    # Use libxml to download the list of S&P500 companies and obtain the symbol table
    #page = lxml.html.parse('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    symbolslist = page.xpath('//table[1]/tr')[1:]
    
    # Obtain the symbol information for each row in the S&P500 constituent table
    symbols = []
    for symbol in symbolslist:
        tds = symbol.getchildren()
        sd = {'ticker': tds[0].getchildren()[0].text,
              'name': tds[1].getchildren()[0].text,
              'sector': tds[3].text}
        # Create a tuple (for the DB format) and append to the grand list
        symbols.append( (sd['ticker'], 'stock', sd['name'], 
                         sd['sector'], 'USD', now, now) )
    return symbols

if __name__ == "__main__":
    db = init_db()
    symbols = obtain_parse_wiki_snp500()
    insert_snp500_symbols(db, symbols)
    db.close()
