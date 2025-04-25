'''
===========================================================================================================================================
ELT dữ liệu stg_collection (Nguồn Data: stg_tmdb_json_movie_metadata)
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

co = movie_metadata[['id','belongs_to_collection']]
co.replace([None],[np.nan],inplace=True)

co_list = []
error_list = []

image_https = 'https://image.tmdb.org/t/p/w500'

for i in tqdm(set(co['id']),desc= 'Processing data'):
    try:
        datastr = co[co['id']==i]['belongs_to_collection'].values[0]
        datastr['movieid'] = i
        co_list.append(datastr)
    except:
        error_list.append(i)
        
codf = pd.DataFrame(co_list)
codf.replace([None],[np.nan],inplace=True)
codf['keyid'] = codf['movieid'].astype('str') + codf['id'].astype('str')
codf['poster_path'] = codf['poster_path'].apply(lambda x: image_https + str(x) if pd.isna(x) == False else x)
codf['backdrop_path'] = codf['backdrop_path'].apply(lambda x: image_https + str(x) if pd.isna(x) == False else x)
codf.rename(columns={'id':'collection_id','movieid':'id'},inplace=True)
codf = codf[['keyid','collection_id','id','name','poster_path','backdrop_path']]
codf.sort_values(by='collection_id',inplace=True)

use_core.sql_insert_py(
                        sql_table_name='staging.stg_collection',
                        python_table=codf,
                        inplace= True                         
                      )