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
movie_json.sort_values(by='id',inplace=True)

data_dict_list = []

for i in tqdm(set(movie_json['id'])):
    data_str = movie_json[movie_json['id']==i]['request_json'].values[0]
    data = f'''{data_str}'''
    data_dict = json.loads(data)
    data_dict_list.append(data_dict)
    
movie_metadata = pd.DataFrame(data_dict_list)
movie_metadata.sort_values(by='id',inplace=True)
movie_metadata = movie_metadata[['id','title','original_title','original_language','release_date','status','overview','tagline','adult','popularity','homepage','poster_path','runtime','budget','revenue','vote_average','vote_count']]

use_core.sql_insert_py(
                        sql_table_name='staging.stg_movie_metadata',
                        python_table=movie_metadata,
                        inplace= True                        
                      )