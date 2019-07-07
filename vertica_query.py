import pyodbc
import pandas
import struct
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd


def getDWHData(command, servername, port, database, schema, username, password):
    """Make sure you have installed the 32bit driver for DB2 on your Windows10 computer for the driver to read correctly"""
    dwh = pyodbc.connect('Driver={IBM DB2 ODBC Driver}; '
    'Hostname=' + servername + '; '
    'Port=' + port + '; '
    'Protocol=TCPIP; '
    'Database=' + database + '; '
    'CurrentSchema=' + schema + '; '
    'UID=' + username + '; '
    'PWD =' + password + '; '
    )
    data = pandas.read_sql(command, dwh)
    return data

def getData(selectStr):
    """
    INPUT: A SQL statement that can be sent to Vertica
    OUTPUT: Results from the SQL statement as a dataframe
    PURPOSE: To query a vetica database. Helps with automating business processes or machine learning projects that involve updated data
    """
    
    tempDF=pd.DataFrame()
    conn_info = {'host': 'host address',
             'port': port number,
             'user': 'user id',
             'password': 'credentials',
             'database': 'name of database',
             # 10 minutes timeout on queries
             'read_timeout': 1000,
             # default throw error on invalid UTF-8 results
             'unicode_error': 'strict',
             # SSL is disabled by default
             'ssl': False,
             'connection_timeout': 100
         }
    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()
    cur.execute(selectStr)
    #get the data
    data_rows=cur.fetchall()
    #dataframe!
    tempDF = pd.DataFrame(data_rows)
    #column Names
    ColumnList = [d.name for d in cur.description]
    #name them correctly
    tempDF.columns=ColumnList
    connection.close()
    return tempDF

def date_months_prior(period):
    """
    INPUT: Number of months from today's date you want to go back for forward: For example if month_prior = 2 then the query will look for the date two months prior to today's date
    OUTPUT: A string with the correct DB2 formatting you need to run query
    """
    return ((datetime.date.today() - relativedelta(months=period)).strftime("%Y-%m-%d").upper())
