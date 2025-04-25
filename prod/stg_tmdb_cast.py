'''
===========================================================================================================================================
ELT dữ liệu stg_cast (Nguồn Data: stg_tmdb_json_movie_credits)
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
# import sys

# if sys.platform == "win32":
#     sys.stdout.reconfigure(encoding='utf-8')

import psycopg2

from core_function import *

use_core = coreproc

q = '''
    with 
        metadata as
            (
                select distinct *
                from staging.stg_tmdb_json_movie_metadata stjmm 
                where request_json = '{"success": false, "status_code": 34, "status_message": "The resource you requested could not be found."}'
                order by id		
            )
    select distinct *
    from staging.stg_tmdb_json_movie_credits stjmc 
    where id not in (select distinct id from metadata)
        and request_json <> '{"success": false, "status_code": 34, "status_message": "The resource you requested could not be found."}'
    order by id 
    '''
creadit = coreuat.selectdf(q)

cast_crew_list = []

for i in tqdm(set(creadit['id']),total=len(set(creadit['id'])),desc='Import data'):
    datastr = creadit[creadit['id']==i]['request_json'].values[0]
    data = json.loads(f'''{datastr}''')
    cast_crew_list.append(data)
   
creaditdf = pd.DataFrame(cast_crew_list)
creaditdf.sort_values(by='id',inplace=True)

cast_raw = creaditdf[['id','cast']]

castdf_list = []

listrun = creaditdf['id']

# listrun = [2]

for i in tqdm(set(listrun),total=len(set(listrun)),desc='Processing data'):
    dataf = cast_raw[cast_raw['id']==i]['cast'].values[0]
    numb_cast = len(dataf)
    for y in range(len(dataf)):
        cast_id = dataf[y]['id']
        dataf[y]['movieid'] = i
        if dataf[y]['profile_path'] is not None:
            dataf[y]['profile_path'] = 'https://image.tmdb.org/t/p/w500' + dataf[y]['profile_path']
        castdf_list.append(dataf[y])

stg_cast = pd.DataFrame(castdf_list)
stg_cast = stg_cast[stg_cast.known_for_department == 'Acting']
print(f'''credit_id' duplicate = {stg_cast['credit_id'].duplicated().sum()}''')
stg_cast = stg_cast[['credit_id', 'movieid','id', 'cast_id', 'character']]
stg_cast.rename(columns={'movieid':'id','id':'actor_id'},inplace=True)

use_core.sql_insert_py(
                        sql_table_name='staging.stg_cast',
                        python_table=stg_cast,
                        inplace= True                         
                      )