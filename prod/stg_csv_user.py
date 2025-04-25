'''
===========================================================================================================================================
ETL bảng stg_user từ file CSV
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

pathfolder = r'F:\Work\Caohoc_2024_2026\PTPM_project\ml-latest'

userinf = pd.read_csv(os.path.join(pathfolder,'users_inf.csv'))

coreproc.sql_insert_py(
         sql_table_name='staging.stg_user',
         python_table=userinf,
         inplace= True
        )
