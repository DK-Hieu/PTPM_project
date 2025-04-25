'''
===========================================================================================================================================
ELT dữ liệu stg_genres (Nguồn Data: stg_tmdb_json_movie_metadata)
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

use_core = coreproc

sql_check = '''
            select distinct *
            from staging.stg_tmdb_json_movie_metadata stjmm 
            where request_json <> '{"success": false, "status_code": 34, "status_message": "The resource you requested could not be found."}'
            order by id
            '''
movie_json = use_core.selectdf(sql_check)
# list_c['id'] = list_c['id'].astype(str)  # Đảm bảo cùng kiểu dữ liệu
movie_json.sort_values(by='id',inplace=True)

data_dict_list = []

for i in tqdm(set(movie_json['id']),desc= 'Input data'):
    data_str = movie_json[movie_json['id']==i]['request_json'].values[0]
    data = f'''{data_str}'''
    data_dict = json.loads(data)
    data_dict_list.append(data_dict)
    
movie_metadata = pd.DataFrame(data_dict_list)
movie_metadata.sort_values(by='id',inplace=True)

g = movie_metadata[['id','genres']]
g.replace([None],[np.nan],inplace=True)

g_list = []

listrun = set(g['id'])

for i in tqdm(listrun, total= len(listrun),desc= 'Processing data'):
    dataf = g[g['id']==i]['genres'].values[0]
    for y in range(len(dataf)):
        dataf[y]['movieid'] = i
        g_list.append(dataf[y])   
        
genres = pd.DataFrame(g_list)
genres.replace([None],[np.nan],inplace=True)
genres.drop_duplicates(inplace=True)
genres.sort_values(by=['movieid','id'],inplace=True)
genres['keyid'] = genres['movieid'].astype('str') + genres['id'].astype('str')
genres = genres[['keyid','movieid','name']]
genres.rename(columns={'movieid':'id','name':'genres'},inplace=True)

use_core.sql_insert_py(
                        sql_table_name='staging.stg_genres',
                        python_table=genres,
                        inplace= True                         
                      )