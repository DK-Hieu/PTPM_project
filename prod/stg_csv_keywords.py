'''
===========================================================================================================================================
2025-04-17 - hieudd - code crawl dữ liệu rating từ dataset .csv vào database staging.stg_keyword
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

from core_function import *

pathfolder = r'F:\Work\Caohoc_2024_2026\PTPM_project\ml-latest'

moives_tags = pd.read_csv(os.path.join(pathfolder,'tags.csv'))
moives_tags['date_tag'] = pd.to_datetime(moives_tags['timestamp'],unit='s')
moives_tags['tag'] = moives_tags['tag'].str.lower().str.strip()
moives_tags['tag'] = moives_tags['tag'].replace('',np.nan)
moives_tags.rename(columns={'tag':'keywords'},inplace=True)
moives_tags['keyid'] = moives_tags['userId'].astype('int').astype('str') + moives_tags['movieId'].astype('int').astype('str') + moives_tags['timestamp'].astype('int').astype('str')
moives_tags = moives_tags[['keyid','userId','movieId','keywords','date_tag']]
moives_tags = moives_tags.head(20000)

print(moives_tags.isnull().sum())

coreproc.sql_insert_py(
            sql_table_name= 'staging.stg_keyword',
            python_table= moives_tags,
            inplace= True
        )