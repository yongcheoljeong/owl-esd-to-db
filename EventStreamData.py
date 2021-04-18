import pandas as pd 
import numpy as np 
import gzip 
import os 
import sys 
import csv 
import json 
import time 
from tqdm import tqdm 
import pymysql 
from pandas.io.json import json_normalize
import collections 

from GUID import *
import mysql_auth 



class EventStreamData: 
    def __init__(self):
        self.set_directory()
    
    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 
    
    def read_data(self):
        pass 
    
    def process_data(self):
        pass 
    
    def get_data(self):
        pass 

# GameInfo
class GameInfo(EventStreamData):
    def __init__(self):
        self.set_directory()
    
    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 

    def read_data(self): 
        data_tag = 'payload_gameinfo'
        filelist = os.listdir(self.root_dir)
        zipfilename = [x for x in filelist if data_tag in x][0]

        data = []
        with gzip.open(os.path.join(self.root_dir, zipfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                esports_match_id = row['esports_match_id']
                time = row['time']
                num_map = json.loads(row['info'])['esports_ids']['esports_match_game_number']
                map_guid = json.loads(row['info'])['game_context']['map_guid']
                map_type = json.loads(row['info'])['game_context']['map_type']
                num_round = json.loads(row['info'])['game_context']['round']
                if map_type == 'CONTROL':
                    round_name = json.loads(row['round_name'])['en_us']
                else:
                    round_name= ''
                team_one_esports_team_id = json.loads(row['score_info'])['team_info'][0]['team']['esports_team_id']
                team_two_esports_team_id = json.loads(row['score_info'])['team_info'][1]['team']['esports_team_id']
                attacking_team_id = json.loads(row['score_info'])['attacking_team']['esports_team_id']
                team_one_score = json.loads(row['score_info'])['team_info'][0]['score']
                team_two_score = json.loads(row['score_info'])['team_info'][1]['score']
                team_one_payload_distance = json.loads(row['score_info'])['team_info'][0]['payload_distance']
                team_two_payload_distance = json.loads(row['score_info'])['team_info'][1]['payload_distance']
                team_one_time_banked = json.loads(row['score_info'])['team_info'][0]['time_banked']
                team_two_time_banked = json.loads(row['score_info'])['team_info'][1]['time_banked']
                context = json.loads(row['context'])['type'] # {"ROUND_END", "SNAPSHOT"}

                output_dict = {
                'esports_match_id':esports_match_id,
                'time':time,
                'num_map':num_map,
                'map_name':map_guid,
                'map_type':map_type,
                'num_round':num_round,
                'round_name':round_name,
                'team_one_name':team_one_esports_team_id,
                'team_two_name':team_two_esports_team_id,
                'attacking_team_name':attacking_team_id,
                'team_one_score':team_one_score,
                'team_two_score':team_two_score,
                'team_one_payload_distance':team_one_payload_distance,
                'team_two_payload_distance':team_two_payload_distance,
                'team_one_time_banked':team_one_time_banked,
                'team_two_time_banked':team_two_time_banked,
                'context':context
                }

                data.append(output_dict)

            df_raw = pd.DataFrame(data)
            self.df_raw = df_raw

    def process_data(self): 
        self.read_data()
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        # team name
        team_guid = GUIDTeam().get_dict()
        processed_data['team_one_name'] = processed_data['team_one_name'].astype(str).replace(team_guid)
        processed_data['team_two_name'] = processed_data['team_two_name'].astype(str).replace(team_guid)
        processed_data['attacking_team_name'] = processed_data['attacking_team_name'].astype(str).replace(team_guid)

        self.processed_data = processed_data

    def get_data(self): 
        self.process_data()
        return self.processed_data

# GameStart
class GameStart(EventStreamData):
    def __init__(self):
        self.set_directory()
    
    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 
        
    def read_data(self):
        data_tag = 'payload_gamestart'
        filelist = os.listdir(self.root_dir)
        zipfilename = [x for x in filelist if data_tag in x][0]

        data = []
        with gzip.open(os.path.join(self.root_dir, zipfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                esports_match_id = row['esports_match_id']
                time = row['time']
                num_map = json.loads(row['info'])['esports_ids']['esports_match_game_number']
                map_guid = json.loads(row['info'])['game_context']['map_guid']
                map_type = json.loads(row['info'])['game_context']['map_type']

                output_dict = {
                'esports_match_id':esports_match_id,
                'start_time':time,
                'num_map':num_map,
                'map_name':map_guid,
                'map_type':map_type,
                }

                data.append(output_dict)

            df_raw = pd.DataFrame(data)

            # END_REASON != NORMAL인 맵 제거 --> 재경기를 하게 되므로 num_map이 같음
            idx = df_raw[df_raw['num_map'].diff() == 0].index - 1 # num_map이 연속되면 앞 시간 맵을 제거
            df_raw.drop(idx, inplace=True)
            df_raw = df_raw.reset_index(drop=True)

            self.df_raw = df_raw

    def process_data(self): 
        self.read_data()
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        self.processed_data = processed_data

    def get_data(self): 
        self.process_data()
        return self.processed_data

# GameResult
class GameResult(EventStreamData):
    def __init__(self):
        self.set_directory()
    
    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 

    def read_data(self): 
        def whoismatchwinner(winningteamid):
            if winningteamid == team_one_team_id:
                winner_esports_team_id = team_one_esports_team_id
            elif winningteamid == team_two_team_id:
                winner_esports_team_id = team_two_esports_team_id
            else:
                winner_esports_team_id = 0 # Draw
            
            return winner_esports_team_id 
        
        data_tag = 'payload_gameresult'
        filelist = os.listdir(self.root_dir)
        zipfilename = [x for x in filelist if data_tag in x][0]

        data = []
        with gzip.open(os.path.join(self.root_dir, zipfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                if row['end_reason'] == 'NORMAL': # get map result only when the map ended normally
                    esports_match_id = row['esports_match_id']
                    time = row['time']
                    winningteamid = row['winningteamid']
                    total_game_time_ms = row['total_game_time_ms']
                    num_map = json.loads(row['info'])['esports_ids']['esports_match_game_number']
                    map_guid = json.loads(row['info'])['game_context']['map_guid']
                    map_type = json.loads(row['info'])['game_context']['map_type']
                    team_one_esports_team_id = json.loads(row['score_info'])['team_info'][0]['team']['esports_team_id']
                    team_two_esports_team_id = json.loads(row['score_info'])['team_info'][1]['team']['esports_team_id']
                    team_one_team_id = json.loads(row['score_info'])['team_info'][0]['team_id']
                    team_two_team_id = json.loads(row['score_info'])['team_info'][1]['team_id']
                    team_one_score = json.loads(row['score_info'])['team_info'][0]['score']
                    team_two_score = json.loads(row['score_info'])['team_info'][1]['score']
                    winner_esports_team_id = whoismatchwinner(winningteamid)

                    output_dict = {
                    'esports_match_id':esports_match_id,
                    'end_time':time,
                    'total_game_time':(int(total_game_time_ms) / 1000),
                    'num_map':num_map,
                    'map_name':map_guid,
                    'map_type':map_type,
                    'map_winner':winner_esports_team_id,
                    'team_one_name':team_one_esports_team_id,
                    'team_two_name':team_two_esports_team_id,
                    'team_one_score':team_one_score,
                    'team_two_score':team_two_score,
                    }

                    data.append(output_dict)
                
                else:
                    pass 

            df_raw = pd.DataFrame(data)
            self.df_raw = df_raw

    def process_data(self): 
        self.read_data()
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        # team name
        team_guid = GUIDTeam().get_dict()
        processed_data['map_winner'] = processed_data['map_winner'].astype(str).replace(team_guid)
        processed_data['team_one_name'] = processed_data['team_one_name'].astype(str).replace(team_guid)
        processed_data['team_two_name'] = processed_data['team_two_name'].astype(str).replace(team_guid)

        # Draw
        processed_data['map_winner'] = processed_data['map_winner'].replace({0,'Draw'})

        ## add start_time from GameStart
        # import gamestart
        gamestart = GameStart()
        gamestart.set_directory(root_dir=self.root_dir)
        df_gamestart = gamestart.get_data()
        # merge with common info
        processed_data = processed_data.set_index(['esports_match_id', 'num_map', 'map_name', 'map_type'])
        df_gamestart = df_gamestart.set_index(['esports_match_id', 'num_map', 'map_name', 'map_type'])
        df_merge = pd.merge(df_gamestart, processed_data, how='inner', left_index=True, right_index=True)
        df_merge = df_merge.reset_index()
        df_merge = df_merge[['esports_match_id', 'num_map', 'map_name', 'map_type', 'start_time', 'end_time', 
'total_game_time', 'map_winner', 'team_one_name', 'team_two_name', 'team_one_score', 'team_two_score']]

        processed_data = df_merge

        self.processed_data = processed_data

    def get_data(self): 
        self.process_data()
        return self.processed_data

# Kill
class Kill(EventStreamData):
    def __init__(self):
        self.set_directory()
    
    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 

    def read_data(self): 
        data_tag = 'payload_kill'
        filelist = os.listdir(self.root_dir)
        zipfilename = [x for x in filelist if data_tag in x][0]

        data = []
        with gzip.open(os.path.join(self.root_dir, zipfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                esports_match_id = json.loads(row['info'])['esports_ids']['esports_match_id']
                time = row['time']
                num_map = json.loads(row['info'])['esports_ids']['esports_match_game_number']
                map_name = json.loads(row['info'])['game_context']['map_guid']
                map_type = json.loads(row['info'])['game_context']['map_type']
                killed_player_id = json.loads(row['killed_player_id'])['seq']
                killed_player_hero_name = row['killed_player_hero_guid']
                final_blow_player_id = json.loads(row['final_blow_player_id'])['seq']
                death_position = (json.loads(row['death_position'])['x'], 
                json.loads(row['death_position'])['y'],
                json.loads(row['death_position'])['z'])
                killer_position = (json.loads(row['killer_position'])['x'], 
                json.loads(row['killer_position'])['y'],
                json.loads(row['killer_position'])['z'])
                killed_pet = row['killed_pet']

                output_dict = {
                    'esports_match_id':esports_match_id,
                    'time':time,
                    'num_map':num_map,
                    'map_name':map_name,
                    'map_type':map_type,
                    'killed_player_id':killed_player_id,
                    'killed_player_hero_name':killed_player_hero_name,
                    'final_blow_player_id':final_blow_player_id,
                    'death_position':death_position,
                    'killer_position':killer_position,
                    'killed_pet':killed_pet
                    }

                data.append(output_dict)

            df_raw = pd.DataFrame(data)
            self.df_raw = df_raw

    def process_data(self): 
        self.read_data()
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        self.processed_data = processed_data

    def get_data(self): 
        self.process_data()
        return self.processed_data


# PlayerStatus
class PlayerStatus(EventStreamData):
    def __init__(self):
        self.set_directory()
    
    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 

    def read_data(self): 
        data_tag = 'payload_playerstatus'
        filelist = os.listdir(self.root_dir)
        zipfilename = [x for x in filelist if data_tag in x][0]

        data = []
        with gzip.open(os.path.join(self.root_dir, zipfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                esports_match_id = row['esports_match_id']
                time = row['time']
                num_map = json.loads(row['info'])['esports_ids']['esports_match_game_number']
                map_name = json.loads(row['info'])['game_context']['map_guid']
                map_type = json.loads(row['info'])['game_context']['map_type']

                statuses = json.loads(row['statuses'])

                for player in statuses:
                    team_name = player['team']['esports_team_id']
                    player_name = player['player']['battletag'].split('#')[0]
                    hero_name = player['name_guid']
                    health = player['health'] + player['armor'] + player['shields']
                    ultimate_percent = player['ultimate_percent']
                    is_ultimate_ready = player['is_ultimate_ready']
                    if player['is_dead'] == False:
                        is_alive = True 
                    elif player['is_dead'] == True or health == 0:
                        is_alive = False 
                    position = (player['position']['x'], player['position']['y'], player['position']['z'])

                    output_dict = {
                        'esports_match_id':esports_match_id,
                        'time':time,
                        'num_map':num_map,
                        'map_name':map_name,
                        'map_type':map_type,
                        'team_name':team_name,
                        'player_name':player_name,
                        'hero_name':hero_name,
                        'health':health,
                        'ultimate_percent':ultimate_percent,
                        'is_ultimate_ready':is_ultimate_ready,
                        'is_alive':is_alive,
                        'position':position
                    }
                    
                    data.append(output_dict)

            df_raw = pd.DataFrame(data)
            self.df_raw = df_raw

    def process_data(self): 
        self.read_data()
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        # team name
        team_guid = GUIDTeam().get_dict()
        processed_data['team_name'] = processed_data['team_name'].astype(str).replace(team_guid)

        # hero name
        name_guid = GUIDHero().get_dict()
        processed_data['hero_name'] = processed_data['hero_name'].astype(str).replace(name_guid)


        self.processed_data = processed_data

    def get_data(self): 
        self.process_data()
        return self.processed_data


class PlayerHeroStats:
    
    def __init__(self):
        self.set_directory()

    def set_directory(self, root_dir=r'D:\2021_EventStreamData'): 
        self.root_dir = root_dir 
        self.NYXL_DB_login = mysql_auth.NYXLDB_ESD_PHS # DB login info
    
    def read_and_dump_data(self): # RAM을 초과하는 대용량 데이터이기 때문에 read & dump를 동시에 수행
        data_tag = 'payload_playerherostats'
        filelist = os.listdir(self.root_dir)
        zipfilename = [x for x in filelist if data_tag in x][0]

        ## Connect to MySQL DB
        # Credentials to DB connection
        hostname = self.NYXL_DB_login['hostname'] 
        username = self.NYXL_DB_login['username']
        pwd = self.NYXL_DB_login['pwd']
        dbname = 'test' # self.NYXL_DB_login['dbname']
        charset = self.NYXL_DB_login['charset']

        # Create connection to MySQL DB
        conn = pymysql.connect(host=hostname, user=username, password=pwd, db=dbname, charset=charset)
        cur = conn.cursor()

        ## Get guids
        # name_guid
        hero_guid = GUIDHero().get_dict()
        # stat_guid
        stat_guid = GUIDStat().get_dict()
        # map_guid
        map_guid = GUIDMap().get_dict()
        # team_guid
        team_guid = GUIDTeam().get_dict()

        ## Calculate the running time
        start_time = time.time()

        ## Define funcs and vars
        def get_table_names(name):
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{name}';"
            
            cur.execute(query) 
            tables = cur.fetchall()
            table_names = list()
            for t in tables:
                table_names.append(t[0])
            
            return table_names

        table_names = get_table_names(dbname)

        # {'table_name': [], 'table_name2': [] ...}
        table_data_list = collections.defaultdict(list)

        # add to table_names list

        def create_table(name):
            query = f'''CREATE TABLE `{name}` (
            `index` int NOT NULL AUTO_INCREMENT,
            `time` varchar(15) DEFAULT NULL,
            `hero_name` varchar(20) DEFAULT NULL,
            `stat_lifespan` varchar(20) DEFAULT NULL,
            `ssg` float DEFAULT NULL,
            `amount` float DEFAULT NULL,
            `stat_name` varchar(100) DEFAULT NULL,
            `player_name` varchar(30) DEFAULT NULL,
            `team_name` varchar(30) DEFAULT NULL,
            PRIMARY KEY (`index`),
            KEY `time` (`time`)
            )'''
            cur.execute(query)

        def delete_table(name):
            query = f'DROP TABLE `{name}`;'
            cur.execute(query)

        ## Read csv file and insert into the DB
        n = 0
        flagTableAlreadyExist = False 

        with gzip.open(os.path.join(self.root_dir, zipfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')

            data = []
            for row in tqdm(csv_reader, desc = 'num row'):
                n += 1
                esports_match_id = row['esports_match_id'] # esports_match_id 따로 빼서 비교
                table_name = f'match_{esports_match_id}'
                
                # time
                utc_time = row['time']
                data.append(utc_time)
                # hero_name
                hero_name = hero_guid.get(row['hero_guid'])
                data.append(hero_name)
                # stat_lifespan
                stat_lifespan = row['stat_lifespan']
                data.append(stat_lifespan)
                
                stat_json = json.loads(row['stat'])
                ssg = stat_json['short_stat_guid']
                # ssg
                data.append(ssg)
                # amount
                stat_amount = stat_json['amount']
                data.append(stat_amount)
                # stat_name
                stat_name = stat_guid.get(ssg)
                data.append(stat_name)

                # player_name
                player_name = json.loads(row['player'])['battletag'].split('#')[0]
                data.append(player_name)

                # team_name
                team_name = team_guid.get(json.loads(row['team'])['esports_team_id'])
                data.append(team_name)
                                            
                table_data_list[table_name].append(tuple(data))

                if n == 1000000: # insert to the DB every 100000 interations
                    for each_table in table_data_list.keys():
                        if (table_name) not in table_names:
                            # create table in mysql here
                            print(f'creating new table: {table_name}')
                            create_table(table_name) # create table as 'match_{esports_match_id}'
                            table_names.append(table_name)

                        sql = f"INSERT INTO {dbname}.{each_table} (time, hero_name, stat_lifespan, ssg, amount, stat_name, player_name, team_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                        cur.executemany(sql, table_data_list[each_table])
                        table_data_list[each_table].clear() # reset the dict 
                    
                    conn.commit()
                    n = 0
                    continue

        # insert rest of the data
        if n != 0:
            # for each_table in table_names:
            for each_table in table_data_list.keys():
                sql = f"INSERT INTO {dbname}.{each_table} (time, hero_name, stat_lifespan, ssg, amount, stat_name, player_name, team_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cur.executemany(sql, table_data_list[each_table])
                table_data_list[each_table].clear() # reset the dict 
            n = 0

        conn.commit()
        cur.close()
        conn.close()
        end_time = time.time() - start_time
        print(f'Time taken: {end_time}s')