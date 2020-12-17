import csv
from tqdm import tqdm
import time
import json
import pymysql
from sqlalchemy import create_engine

# Credentials to DB connection

hostname = "localhost" 
username = "root"
pwd = "gpdlzjadh"
dbname = "esd_phs"
charset = "utf8"

# Create connection to MySQL DB

conn = pymysql.connect(host=hostname, user=username, password=pwd, db=dbname, charset=charset)
cur = conn.cursor()

# file dir setting

filename = 'payload_playerherostats-20200831-20200906.tsv/payload_playerherostats-20200831-20200906.tsv'

# headers = ['time', 'hero_guid', 'stat_lifespan', 'stat', 'player', 'team', 'esports_match_id']
headers_asis = ['time', 'hero_guid', 'stat_lifespan', 'esports_match_id']
json_type_headers = ['stat', 'player', 'team']
stat_json_interest_headers = ['short_stat_guid', 'amount']
player_json_interest_headers = ['battletag', 'esports_player_id']
team_json_interest_headers = ['esports_team_id']

start_time = time.time()
whole_data = []
n = 0
with open(filename, newline='') as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter = '\t')
    
    for row in tqdm(csv_reader, desc = 'num row'):
        n += 1
        data = [row[x] for x in headers_asis]
        stat_json = json.loads(row['stat'])
        data.extend([stat_json[x] for x in stat_json_interest_headers])
        player_json = json.loads(row['player'])
        data.extend([player_json[x] for x in player_json_interest_headers])
        team_json = json.loads(row['team'])
        data.extend([team_json[x] for x in team_json_interest_headers])
        
        
        whole_data.append(data)

        if n == 100000: # insert to db every 10000 interations
            sql = "INSERT INTO esd_phs.phs_raw (time, hero_guid, stat_lifespan, esports_match_id, short_stat_guid, amount, battletag, esports_player_id, esports_team_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cur.executemany(sql, tuple(whole_data))
            n = 0
            whole_data = []
            
            continue

# insert rest of the data
if len(whole_data) != 0:
    sql = "INSERT INTO esd_phs.phs_raw (time, hero_guid, stat_lifespan, esports_match_id, short_stat_guid, amount, battletag, esports_player_id, esports_team_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(sql, tuple(whole_data))
    whole_data = []

conn.commit()
cur.close()
conn.close()
end_time = time.time() - start_time
print(end_time)