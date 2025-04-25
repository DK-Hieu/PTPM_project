'''
===========================================================================================================================================
ETL bảng stg_ratings từ CSV
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

usecore = coreproc

q = '''
        select distinct movieid
        from staging.stg_links
        order by movieid 
    '''

stg_links = usecore.selectdf(q)
stg_links_list = list(set(stg_links['movieid'].values))
stg_links_list.sort()

pathfolder = r'F:\Work\Caohoc_2024_2026\PTPM_project\ml-latest'

movies_ratings = pd.read_csv(os.path.join(pathfolder,'ratings.csv'))
movies_ratings['keyid'] = movies_ratings['userId'].astype('str') + \
                          movies_ratings['movieId'].astype('str') + \
                          movies_ratings['timestamp'].astype('str')
movies_ratings['date_rate'] = pd.to_datetime(movies_ratings['timestamp'],unit='s')
del movies_ratings['timestamp']
movies_ratings.sort_values(by=['date_rate','movieId','userId'],inplace=True)
movies_ratings = movies_ratings[['keyid','userId','movieId','rating','date_rate']]
movies_ratings = movies_ratings[movies_ratings['movieId'].isin(stg_links_list)]
movies_ratings.rename(columns={'movieId':'movieid','userId':'userid'},inplace=True)

print(movies_ratings.isnull().sum())

coreproc.sql_insert_py(
            sql_table_name= 'staging.stg_ratings',
            python_table= movies_ratings,
            inplace= True
        )