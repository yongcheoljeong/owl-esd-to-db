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


# %%
# 경로 설정
# zdir = r'/zipfiledirectory'
filename = 'payload_gameinfo-20200831-20200906.tsv.gz'


# %%
df_raw = pd.read_csv(filename, delimiter = '\t')[['time', 'info', 'score_info', 'esports_match_id']]
df_raw = df_raw.sort_values(by=['time'], axis = 0)
df_raw = df_raw.reset_index(drop = True)
esports_match_ids = pd.unique(df_raw['esports_match_id'])

# %%
# multiprocessing with Pool

import multiproc

start_time = time.time()

if __name__ == '__main__':
    num_cores = mp.cpu_count()
    pool = mp.Pool(processes = num_cores)
    pool.map(multiproc.multiproc, esports_match_ids)
    pool.close()
    pool.join()

end_time = time.time()- start_time
print(f'multiprocess time: {end_time}sec)



