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
    
    def disconnect():
        try:
            if coreproc.cursor:
                coreproc.cursor.close()
            if coreproc.conn:
                coreproc.conn.close()
            print("Disconnected from database.")
        except Exception as e:
            print(f"Lỗi khi đóng kết nối: {e}")    
        
    
    def truncate_table(sql_table_name):
        sql_query = f'truncate table {sql_table_name}'
        coreproc.cursor.execute(sql_query)
        coreproc.conn.commit()        
    
    def primary_key_take(sql_table_name):
        q = f'''
                SELECT
                    a.attname AS column_name
                FROM
                    pg_index i
                JOIN
                    pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN
                    pg_class c ON c.oid = i.indrelid
                JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    i.indisprimary = true
                    AND (n.nspname || '.' || c.relname) = '{sql_table_name}';
            '''
        column_name = coreproc.selectdf(q)
        return column_name['column_name'][0]
        
        
    def sql_append_check(sql_table_name,python_table):
        pk = coreproc.primary_key_take(sql_table_name)
        q = f'''select distinct {pk} from {sql_table_name}'''
        sqldf = coreproc.selectdf(q)
        sqlkeyid = set(sqldf[f'{pk}'])
        pythonkeyid = set(python_table[f'{pk}'])
        updatelist = list(pythonkeyid - sqlkeyid)
        updatedf = python_table[python_table[f'{pk}'].isin(updatelist)]
        return updatedf        
         
         
    def sql_insert_py(sql_table_name,python_table,inplace):
        
        python_table.replace([np.nan], [None],inplace=True)
        
        if inplace == True:
            coreproc.truncate_table(sql_table_name)
        if inplace == False:
            python_table = coreproc.sql_append_check(sql_table_name,python_table)
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
        coreproc.disconnect()
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
    
    def disconnect():
        try:
            if coreuat.cursor:
                coreuat.cursor.close()
            if coreuat.conn:
                coreuat.conn.close()
            print("Disconnected from database.")
        except Exception as e:
            print(f"Lỗi khi đóng kết nối: {e}") 
    
    def truncate_table(sql_table_name):
        sql_query = f'truncate table {sql_table_name}'
        coreuat.cursor.execute(sql_query)
        coreuat.conn.commit() 
        
    def primary_key_take(sql_table_name):
        q = f'''
                SELECT
                    a.attname AS column_name
                FROM
                    pg_index i
                JOIN
                    pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN
                    pg_class c ON c.oid = i.indrelid
                JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    i.indisprimary = true
                    AND (n.nspname || '.' || c.relname) = '{sql_table_name}';
            '''
        column_name = coreuat.selectdf(q)
        return column_name
        
        
    def sql_append_check(sql_table_name,python_table):
        pk = coreuat.primary_key_take(sql_table_name)
        q = f'''select distinct {pk} from {sql_table_name}'''
        sqldf = coreuat.selectdf(q)
        sqlkeyid = set(sqldf[f'{pk}'])
        pythonkeyid = set(python_table[f'{pk}'])
        updatelist = list(pythonkeyid - sqlkeyid)
        updatedf = python_table[python_table[f'{pk}'].isin(updatelist)]
        return updatedf  
    
    
    def sql_insert_py(sql_table_name,python_table,inplace):
        
        python_table.replace([np.nan], [None],inplace=True)
        
        if inplace == True:
            coreuat.truncate_table(sql_table_name)
        if inplace == False:
            python_table = coreuat.sql_append_check(sql_table_name,python_table)
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
        coreuat.disconnect()
        print("PUSH DATA: DONE")
        
    