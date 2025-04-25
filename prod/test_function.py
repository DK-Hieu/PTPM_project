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

pathfolder = r'F:\Work\Caohoc_2024_2026\PTPM_project\ml-latest'

links = pd.read_csv(os.path.join(pathfolder,'links.csv'))
links = links[links['tmdbId'].isnull()==False]
links.sort_values(by='tmdbId',inplace=True)
# links['keyid'] = links['movieId'].astype('int').astype('str') + links['imdbId'].astype('int').astype('str') + links['tmdbId'].astype('int').astype('str')
# links = links[['keyid','movieId','imdbId','tmdbId']]
links = links[['movieId','imdbId','tmdbId']]
links.rename(columns={'movieId':'movieid','imdbId':'imdbid','tmdbId':'tmdbid'},inplace=True)
links = links.head(5000)

print(links.isnull().sum())

# print(coreproc.sql_append_check(
#          sql_table_name='staging.stg_links',
#          python_table=links
#                                ))

coreproc.sql_insert_py(
         sql_table_name='staging.stg_links',
         python_table=links,
         inplace= False
        )