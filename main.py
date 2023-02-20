import psycopg2
import os
import sys
import pandas as pd
import pandas.io.sql as sqlio
import datetime

from pipeline import populate_dwh
redshift = {'DWH_DB_USER':os.environ['DWH_DB_USER'],
    'DWH_DB_PASSWORD':os.environ['DWH_DB_PASSWORD'],
    'DWH_ENDPOINT':os.environ['DWH_ENDPOINT'],
    'DWH_PORT':'5439',
    'DWH_DB':'dev',
    'DWH_IAM_ROLE': os.environ['DWH_IAM_ROLE']}


date_range = (datetime.datetime.utcnow() - datetime.timedelta(days = i) for i in range(1,7))
keyword_list = ['ChatGPT','Ohio','Layoff','TSLA']
for date in date_range:
    for keyword in keyword_list:
        print(keyword,datetime.datetime.combine(date,datetime.time.min))
        populate_dwh(keyword=keyword,date = date,max_page = 50)