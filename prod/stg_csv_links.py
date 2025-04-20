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

from core_function import *

pathfolder = r'F:\Work\Caohoc_2024_2026\PTPM_project\ml-latest'

links = pd.read_csv(os.path.join(pathfolder,'links.csv'))
links = links[links['tmdbId'].isnull()==False]
links.sort_values(by='tmdbId',inplace=True)
# links['keyid'] = links['movieId'].astype('int').astype('str') + links['imdbId'].astype('int').astype('str') + links['tmdbId'].astype('int').astype('str')
# links = links[['keyid','movieId','imdbId','tmdbId']]
links = links[['movieId','imdbId','tmdbId']]
links = links.head(20000)

print(links.isnull().sum())

coreproc.sql_insert_py(
         sql_table_name='staging.stg_links',
         python_table=links,
         inplace= True
        )
