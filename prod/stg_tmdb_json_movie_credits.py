'''
===========================================================================================================================================
ELT dữ liệu raw từ TMDB vào stg_tmdb_json_movie_credits /
(Dữ liệu gốc của cast, crew) 
===========================================================================================================================================
'''
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import time
import json
import ast
from tqdm.auto import tqdm
import requests
import warnings
warnings.filterwarnings('ignore') # turn off warnings

import psycopg2

from core_function import *

connUAT = psycopg2.connect(
     host='localhost',
     database='uat',
     port=5431,
     user='postgres',
     password='1234567890'
  )

cursorUAT = connUAT.cursor()

connPROC = psycopg2.connect(
     host='localhost',
     database='prod',
     port=5431,
     user='postgres',
     password='1234567890'
  )

cursorPROC = connPROC.cursor()

#----------------------------------------------

use_core = coreproc 
use_conn = connPROC
use_cursor = cursorPROC

#---------------------------------------------

def check_list(sql_table_object):
    sql_query = f'''
                select distinct id from {sql_table_object}
                '''
    list_sql_object = use_core.selectdf(sql_query)
    list_sql_object.sort_values(by='id',inplace=True)
    
    sql_query = '''
            select distinct tmdbid id from staging.stg_links
            where tmdbid is not null
            '''
    list_sql_all = use_core.selectdf(sql_query)
    list_sql_all.sort_values(by='id',inplace=True)
    
    list_crwal = list(set(list_sql_all['id']) - set(list_sql_object['id']))
    list_crwal.sort()
    return list_crwal

# Movie_credits

sql_table_name = 'staging.stg_tmdb_json_movie_credits'

list_m = check_list(sql_table_name)

for i in tqdm(list_m):
    try:
        time.sleep(0.01)
        tmdb_id = i

        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits?language=en-US"

        headers = {
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNWEzMGJmNWQyYmU5YzdiNzUwNTQ5ZTc3NTc1YTQ5OCIsIm5iZiI6MTc0Mjc0ODk0Ny44LCJzdWIiOiI2N2UwM2QxMzIxMGZhODBhMGY0ZGE4NzMiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.3Jf337Oe9cB_nBG4kCFfxnLYNpEXcm13G92QoBJSf2k"     
                }

        response = requests.get(url, headers=headers)
        data = response.json()
        data_str = json.dumps(data)

        sql_q = f"INSERT INTO {sql_table_name} (id, request_json) VALUES (%s, %s)"

        use_cursor.execute(sql_q,(tmdb_id,data_str))
        use_conn.commit()
    except:
        print(f'Lỗi dữ liệu: {i}')

sql_query = f'''
                UPDATE {sql_table_name}
                SET request_json = REGEXP_REPLACE(request_json, '":\s*""', '": null', 'g')
                WHERE request_json LIKE '%: ""%';
            '''

use_cursor.execute(sql_query)
use_conn.commit()

use_cursor.close()
use_conn.close()