import json
from datetime import date
import pandas
import time
import timeit
from pathlib import Path
from pandas.io.json import json_normalize
import os
import re
import vertica_python
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
