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
    pass 

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


