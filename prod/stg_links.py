'''
===========================================================================================================================================
2025-04-17 - hieudd - code crawl dữ liệu links từ dataset .csv vào database staging.stg_links
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

pathfolder = r'F:\Work\Caohoc_2024_2026\PTPM_project\ml-latest'

links = pd.read_csv(os.path.join(pathfolder,'links.csv'))
links = links[links['tmdbId'].isnull()==False]
links.sort_values(by='tmdbId',inplace=True)

SQL_push(
         sql_table_name='staging.stg_links',
         python_table=links,
         conn=connPROC,
         cursor=cursorPROC,
         inplace= True
        )
