'''
===========================================================================================================================================
2025-04-17 - hieudd - code crawl dữ liệu Movie_metadata từ TMDB database vào database staging.stg_tmdb_json_movie_metadata
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
from tqdm import tqdm
import requests
import warnings
warnings.filterwarnings('ignore') # turn off warnings

import psycopg2

connUAT = psycopg2.connect(
     host='localhost',
     database='uat',
     port=5431,
     user='postgres',
     password='1234567890'
  )

connPROC = psycopg2.connect(
     host='localhost',
     database='prod',
     port=5431,
     user='postgres',
     password='1234567890'
  )

cursorUAT = connUAT.cursor()
cursorPROC = connPROC.cursor()

def selectdf(query,conn):
    df = pd.read_sql(query, conn)
    return df
  
def SQL_push(sql_table_name,python_table,conn,cursor,inplace):
    python_table.replace([np.nan], [None],inplace=True)
    
    if inplace == True:
        try:
            sql_query = f'truncate table {sql_table_name}'
            cursor.execute(sql_query)
            conn.commit()
        except:
            pass
            
    sql = f"INSERT INTO {sql_table_name} VALUES ({','.join(['%s'] * len(python_table.columns))})"

    data = [tuple(row) for _, row in tqdm(python_table.iterrows(), total=len(python_table), desc="Preparing data")]

    # Chèn dữ liệu với xử lý lỗi
    for row in tqdm(data, total=len(data), desc="Inserting rows"):
        try:
            cursor.execute(sql, row)
        except Exception as e:
            print(f"Lỗi khi chèn dòng {row}: {e}")
            conn.rollback()  # Hủy giao dịch khi có lỗi, tránh PostgreSQL khóa transaction

    # Sau khi xong thì commit lại
    conn.commit()
    print("PUSH DATA: DONE")

def check_list(sql_table_object,connSQL):
    sql_query = f'''
                select distinct id from {sql_table_object}
                '''
    list_sql_object = selectdf(sql_query,connSQL)
    list_sql_object.sort_values(by='id',inplace=True)
    
    sql_query = '''
            select distinct tmdbid id from staging.stg_links
            where tmdbid is not null
            '''
    list_sql_all = selectdf(sql_query,connSQL)
    list_sql_all.sort_values(by='id',inplace=True)
    
    list_crwal = list(set(list_sql_all['id']) - set(list_sql_object['id']))
    list_crwal.sort()
    return list_crwal

# Movie_metadata

sql_table_name = 'staging.stg_tmdb_json_movie_metadata'

list_m = check_list(sql_table_name,connPROC)

for i in tqdm(list_m):
    try:
        time.sleep(0.01)
        tmdb_id = i

        url = f'https://api.themoviedb.org/3/movie/{tmdb_id}'

        headers = {
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNWEzMGJmNWQyYmU5YzdiNzUwNTQ5ZTc3NTc1YTQ5OCIsIm5iZiI6MTc0Mjc0ODk0Ny44LCJzdWIiOiI2N2UwM2QxMzIxMGZhODBhMGY0ZGE4NzMiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.3Jf337Oe9cB_nBG4kCFfxnLYNpEXcm13G92QoBJSf2k"     
                }

        response = requests.get(url, headers=headers)
        data = response.json()
        data_str = json.dumps(data)

        sql_q = f"INSERT INTO {sql_table_name} (id, request_json) VALUES (%s, %s)"

        cursorPROC.execute(sql_q,(tmdb_id,data_str))
        connPROC.commit()
    except:
        print(f'Lỗi dữ liệu: {i}')

sql_query = f'''
                UPDATE {sql_table_name}
                SET request_json = REGEXP_REPLACE(request_json, '":\s*""', '": null', 'g')
                WHERE request_json LIKE '%: ""%';
            '''

cursorPROC.execute(sql_query)
connPROC.commit()

cursorPROC.close()
connPROC.close()
