# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
import zipfile
import os
import json
import time
import multiprocessing as mp
import matplotlib.pyplot as plt
from tqdm import tqdm
import pymysql
from sqlalchemy import create_engine

# dask packages

import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client
# %%
# dir 설정

filename = 'payload_playerherostats-20200831-20200906.tsv/payload_playerherostats-20200831-20200906.tsv'

# setup a local client

num_cores = mp.cpu_count()
client = Client(n_workers = num_cores)

# register progress bar

pbar = ProgressBar()
pbar.register()

# %%
# interest headers 지정

headers = ['time', 'hero_guid', 'stat_lifespan', 'stat', 'player', 'team', 'esports_match_id']
json_type_headers = ['stat', 'player', 'team']
stat_json_interest_headers = ['short_stat_guid', 'amount']
player_json_interest_headers = ['battletag', 'esports_player_id']
team_json_interest_headers = ['esports_team_id']


# %%
def append_json_to_df_phs(df_phs, json_type_headers):
    
    tmp_stat = pd.DataFrame(columns = stat_json_interest_headers)
    tmp_player = pd.DataFrame(columns = player_json_interest_headers)
    tmp_team = pd.DataFrame(columns = team_json_interest_headers)
    for header in json_type_headers:
        for idx in df_phs.index.values:
            if header == 'stat':
                json_text = json.loads(df_phs.loc[idx, header])
                json_norm = pd.json_normalize(json_text)
                tmp_stat = pd.concat([tmp_stat, json_norm], ignore_index = True)
            elif header == 'player':
                json_text = json.loads(df_phs.loc[idx, header])
                json_norm = pd.json_normalize(json_text)[player_json_interest_headers]
                tmp_player = pd.concat([tmp_player, json_norm], ignore_index = True)
            elif header == 'team':
                json_text = json.loads(df_phs.loc[idx, header])
                json_norm = pd.json_normalize(json_text)[team_json_interest_headers]
                tmp_team = pd.concat([tmp_team, json_norm], ignore_index = True)
    
    df_phs = pd.concat([df_phs, tmp_stat, tmp_player, tmp_team], axis = 1)
    df_phs.drop(columns = json_type_headers, inplace = True) 

    return df_phs

# %%
chunksize = 10000

df_chunk = pd.read_csv(filename, delimiter = '\t', chunksize = chunksize)

start_time = time.time()

for chunk in tqdm(df_chunk, desc = 'num chunk saved'):
    df_sample = chunk[headers]
    df_sample.reset_index(inplace = True)
    print(df_sample.size)
    new_df = append_json_to_df_phs(df_sample, json_type_headers)
    
    if new_df.isnull().values.any() == False:
        pass
    else:
        print('table에 nan값이 포함되어있습니다')
    
    # Credentials to DB connection

    hostname = "localhost" 
    username = "root"
    pwd = "gpdlzjadh"
    dbname = "esd_phs"
    charset = "utf8"

    # Create SQLAlchemy engine to connect to MySQL DB

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host = hostname, db = dbname, user = username, pw = pwd))

    # Convert df to sql table

    new_df.to_sql(name = dbname, con = engine, if_exists = 'append', index = False, method = 'multi')

end_time = time.time()- start_time
print(end_time)