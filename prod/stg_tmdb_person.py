'''
===========================================================================================================================================
ELT dữ liệu stg_person (Nguồn Data: stg_tmdb_json_person)
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

q = '''
        select distinct *
        from staging.stg_tmdb_json_person stjp 
    '''
per = use_core.selectdf(q)
per_list = []

image_https = 'https://image.tmdb.org/t/p/w500'

for i in tqdm(set(per['id']),desc='Input + Processing data:'):
    datastr = per[per['id']==i]['request_json'].values[0]
    data = json.loads(f'''{datastr}''')
    per_list.append(data)

perdf = pd.DataFrame(per_list)
perdf.replace([None],[np.nan],inplace=True)
perdf.replace([''],[np.nan],inplace=True)
perdf['also_known_as'] = perdf['also_known_as'].apply(lambda x: np.nan if len(x)==0 else x)
perdf.sort_values(by='id',inplace=True)
perdf = perdf[['id', 'imdb_id','name','also_known_as','gender',
                'birthday', 'deathday', 'place_of_birth',
                'known_for_department','popularity','profile_path','biography']]
perdf.rename(columns={'known_for_department':'job','also_known_as':'also_name'},inplace=True)
perdf['profile_path'] = image_https + perdf['profile_path']

use_core.sql_insert_py(
                        sql_table_name='staging.stg_person',
                        python_table=perdf,
                        inplace= True                         
                      )