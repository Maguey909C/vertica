
#Packages to import
import json
from datetime import date
import pandas
import time
import timeit
from pathlib import Path
from pandas.io.json import json_normalize
import os
import re
from io import StringIO
import numpy as np

def createBufferString(df):
    i=len(df.columns)
    s=""
    for number in range(i):
        s+='{}|'
    s=s[:-1]
    s+='\n'
    return s

def getColumns(df):
    buffer=""
    ColumnList = [d for d in df.columns]
    for c in ColumnList:
        buffer+=c + ","
    buffer = buffer[:-1]
    return buffer

def replaceRejectValues(bufferValue):
    bufferValue= bufferValue.replace("None","")
    bufferValue= bufferValue.replace("nan","0")
    bufferValue= bufferValue.replace("+","")
    bufferValue= bufferValue.replace("#","")
    bufferValue= bufferValue.replace("*","")
    bufferValue= bufferValue.replace("unknown","")
    bufferValue= bufferValue.replace("Anonymous","")
    bufferValue= bufferValue.replace("Unavailable","")
    bufferValue= bufferValue.replace("Restricted","")
    return bufferValue

def copyToVertica(df, targetTable):
    """
    INPUT: A dataframe that you want inserted into vertica and the desired table name and schema
    OUTPUT: An insertion of the datafram data into Vertica based on conn_info
    """

    conn_info = {'host': 'host name',
                 'port': port number,
                 'user': 'your user id',
                 'password': 'your password',
                 'database': 'name of database',
                 # 10 minutes timeout on queries
                 'read_timeout': 600,
                 # default throw error on invalid UTF-8 results
                 'unicode_error': 'strict',
                 # SSL is disabled by default
                 'ssl': False,
                 'connection_timeout': 5
                }
    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()
    #add new import:
    #temporary buffer
    buff = StringIO()
    # convert data frame to csv type
    for row in df.values.tolist():
        buff.write(createBufferString(df).format(*row))

    # now insert data
    with connection.cursor() as cursor:
        insertStr='COPY ' + targetTable + ' ( ' + getColumns(df).replace('.','_')  + ') FROM STDIN REJECTED DATA AS table dsl.crenick_loader_rejects;'
        cursor.copy(insertStr, replaceRejectValues(buff.getvalue()))
    connection.commit()
    connection.close()

def clearTable(targetTable):
    """
    INPUT: The target table that will be cleared, but not dropped
    OUTPUT: An executable str that will be executed and committed on Vertica.
    *Note with some modification you could create tables with this query
    """

    conn_info = {'host': 'your host name',
         'port': portnumber,
         'user': 'your userid',
         'password': 'your password',
         'database': 'Name of Database',
         # 10 minutes timeout on queries
         'read_timeout': 600,
         # default throw error on invalid UTF-8 results
         'unicode_error': 'ignore',
         # SSL is disabled by default
         'ssl': False,
         'connection_timeout': 5
     }
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()
    cur.execute('DELETE FROM ' + targetTable)
    connection.commit()
    connection.close()
