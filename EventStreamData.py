import pandas as pd 
import numpy as np 
import gzip 
import os 
import csv 
import json 
import time 
from pandas.io.json import json_normalize
from GUID import *



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


class GameInfo(EventStreamData):
    def __init__(self):
        self.set_directory()
        self.read_data()
        self.process_data()
    
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
        return self.processed_data

class GameResult(EventStreamData):
    def __init__(self):
        self.set_directory()
        self.read_data()
        self.process_data()
    
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
                    'start_time':time,
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

        self.processed_data = processed_data

    def get_data(self): 
        return self.processed_data


class Kill(EventStreamData):
    def __init__(self):
        self.set_directory()
        self.read_data()
        self.process_data()
    
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
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        self.processed_data = processed_data

    def get_data(self): 
        return self.processed_data

class PlayerHeroStat(EventStreamData):
    def __init__(self): 
        pass 

    def set_directory(self):
        pass 

    def read_data(self):
        pass 

    def process_data(self): 
        pass 

    def export_data(self):
        pass 

class PlayerStatus(EventStreamData):
    def __init__(self):
        self.set_directory()
        self.read_data()
        self.process_data()
    
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
                    hero_name = player['hero_guid']
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
        ## transform guid to english name
        # map name
        map_guid = GUIDMap().get_dict()
        processed_data = self.df_raw
        processed_data['map_name'] = self.df_raw['map_name'].astype(str).replace(map_guid)

        # team name
        team_guid = GUIDTeam().get_dict()
        processed_data['team_name'] = processed_data['team_name'].astype(str).replace(team_guid)

        # hero name
        hero_guid = GUIDHero().get_dict()
        processed_data['hero_name'] = processed_data['hero_name'].astype(str).replace(hero_guid)


        self.processed_data = processed_data

    def get_data(self): 
        return self.processed_data