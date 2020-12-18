# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
from tqdm import tqdm
import os
import json
import time
import csv


# %%
# translate stat_guid

df = pd.read_csv('guid/payload_guids_stats.tsv', delimiter = '\t')

tmp_stat = pd.DataFrame()
for row in df.index.values:
    json_text = json.loads(df.loc[row, 'stat_name'])
    stat_name = pd.json_normalize(json_text)
    tmp_stat = pd.concat([tmp_stat, stat_name.en_us], ignore_index = True)

df = pd.concat([df, tmp_stat], axis = 1)
df = df.rename(columns = {0:'en_stat_name'})

df.to_csv('guid/stat_guid.csv', encoding = 'utf-8', index = False)
new_df = pd.read_csv('guid/stat_guid.csv')
new_df


# %%
# translate hero_guid

df = pd.read_csv('guid/payload_guids_heroes.tsv', delimiter = '\t')

tmp_hero = pd.DataFrame()
for row in df.index.values:
    json_text = json.loads(df.loc[row, 'hero'])
    hero_name = pd.json_normalize(json_text)
    tmp_hero = pd.concat([tmp_hero, hero_name.en_US], ignore_index = True)

df = pd.concat([df, tmp_hero], axis = 1)
df = df.rename(columns = {0:'en_hero_name'})

df.to_csv('guid/hero_guid.csv', encoding = 'utf-8', index = False)
new_df = pd.read_csv('guid/hero_guid.csv')
new_df


# %%
# translate map_guid

df = pd.read_csv('guid/payload_guids_maps.tsv', delimiter = '\t')

tmp_map = pd.DataFrame()
for row in df.index.values:
    json_text = json.loads(df.loc[row, 'map_name'])
    map_name = pd.json_normalize(json_text)
    tmp_map = pd.concat([tmp_map, map_name.en_US], ignore_index = True)

df = pd.concat([df, tmp_map], axis = 1)
df = df.rename(columns = {0:'en_map_name'})

df.to_csv('guid/map_guid.csv', encoding = 'utf-8', index = False)
new_df = pd.read_csv('guid/map_guid.csv')
new_df


