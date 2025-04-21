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

class coreproc:
    from tqdm.auto import tqdm

    
    import sys

    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8')

    conn = psycopg2.connect(
        host='localhost',
        database='prod',
        port=5431,
        user='postgres',
        password='1234567890'
    )   

    cursor = conn.cursor()

    def selectdf(query):
        df = pd.read_sql(query, coreproc.conn)
        return df
    
    def truncate_table(sql_table_name):
        sql_query = f'truncate table {sql_table_name}'
        coreproc.cursor.execute(sql_query)
        coreproc.conn.commit()        
    
    def sql_insert_py(sql_table_name,python_table,inplace):
        
        python_table.replace([np.nan], [None],inplace=True)
        
        if inplace == True:
            coreproc.truncate_table(sql_table_name)
        else:
            pass
                
        sql = f"INSERT INTO {sql_table_name} VALUES ({','.join(['%s'] * len(python_table.columns))})"

        data = [tuple(row) for _, row in tqdm(python_table.iterrows(), total=len(python_table), desc="Preparing data")]

        # Chèn dữ liệu với xử lý lỗi
        for row in tqdm(data, total=len(data), desc="Inserting rows"):
            try:
                coreproc.cursor.execute(sql, row)
            except Exception as e:
                print(f"Lỗi khi chèn dòng {row}: {e}")
                coreproc.conn.rollback()  # Hủy giao dịch khi có lỗi, tránh PostgreSQL khóa transaction

        # Sau khi xong thì commit lại
        coreproc.conn.commit()
        print("PUSH DATA: DONE")
        
        

class coreuat:
    from tqdm.auto import tqdm

    import sys

    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8')
    
    conn = psycopg2.connect(
        host='localhost',
        database='uat',
        port=5431,
        user='postgres',
        password='1234567890'
    )   

    cursor = conn.cursor()

    def selectdf(query):
        df = pd.read_sql(query, coreuat.conn)
        return df
    
    def truncate_table(sql_table_name):
        sql_query = f'truncate table {sql_table_name}'
        coreuat.cursor.execute(sql_query)
        coreuat.conn.commit() 
    
    def sql_insert(sql_table_name,python_table,inplace):
        python_table.replace([np.nan], [None],inplace=True)
        
        if inplace == True:
            coreuat.truncate_table(sql_table_name)
        else:
            pass
                
        sql = f"INSERT INTO {sql_table_name} VALUES ({','.join(['%s'] * len(python_table.columns))})"

        data = [tuple(row) for _, row in tqdm(python_table.iterrows(), total=len(python_table), desc="Preparing data")]

        # Chèn dữ liệu với xử lý lỗi
        for row in tqdm(data, total=len(data), desc="Inserting rows"):
            try:
                coreuat.cursor.execute(sql, row)
            except Exception as e:
                print(f"Lỗi khi chèn dòng {row}: {e}")
                coreuat.conn.rollback()  # Hủy giao dịch khi có lỗi, tránh PostgreSQL khóa transaction

        # Sau khi xong thì commit lại
        coreuat.conn.commit()
        print("PUSH DATA: DONE")
        