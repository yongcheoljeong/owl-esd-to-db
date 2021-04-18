import csv
import gzip 
import json
import os 

class GUIDMap:
    def __init__(self):
        self.set_guid()
        self.update_dict()

    def set_guid(self):
        root_dir = r'D:\2021_EventStreamData\guid'
        guidfilename = 'payload_guids_maps.tsv'

        dict_data = dict()
        with open(os.path.join(root_dir, guidfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                map_guid = row['guid']
                map_name = json.loads(row['map_name'])['en_US']

                output_dict = {map_guid:map_name}

                dict_data.update(output_dict)

        self.dict_data = dict_data
    
    def update_dict(self):
        '''
        guid 추가됐을 때 여기서 작업
        '''
        pass 

    def get_dict(self):
        return self.dict_data


class GUIDHero:
    def __init__(self):
        self.set_guid()
        self.update_dict()

    def set_guid(self):
        root_dir = r'D:\2021_EventStreamData\guid'
        guidfilename = 'payload_guids_heroes.tsv'

        dict_data = dict()
        with open(os.path.join(root_dir, guidfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                hero_guid = row['guid']
                hero_name = json.loads(row['hero'])['en_US']

                output_dict = {hero_guid:hero_name}

                dict_data.update(output_dict)

        self.dict_data = dict_data
    
    def update_dict(self):
        '''
        guid 추가됐을 때 여기서 작업
        '''
        torbjorn = {'207165582859042822':'Torbjorn'}
        lucio = {'207165582859042937':'Lucio'}
        echo = {'207165582859043334':'Echo'}
        all_heroes = {'207165587154010111':'All Heroes'}
        not_selected = {'0':None}

        self.dict_data.update(torbjorn)
        self.dict_data.update(lucio)
        self.dict_data.update(echo)
        self.dict_data.update(all_heroes)
        self.dict_data.update(not_selected)

    def get_dict(self):
        return self.dict_data


class GUIDStat:
    def __init__(self):
        self.set_guid()
        self.update_dict()

    def set_guid(self):
        root_dir = r'D:\2021_EventStreamData\guid'
        guidfilename = 'payload_guids_stats.tsv'

        dict_data = dict()
        with open(os.path.join(root_dir, guidfilename), mode='rt', newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter = '\t')
            for row in csv_reader:
                stat_guid = row['ssg']
                stat_name = json.loads(row['stat_name'])['en_us']

                output_dict = {stat_guid:stat_name}

                dict_data.update(output_dict)

        self.dict_data = dict_data
    
    def update_dict(self):
        '''
        guid 추가됐을 때 여기서 작업
        '''

    def get_dict(self):
        return self.dict_data

class GUIDTeam:
    def __init__(self):
        self.dict_data = {
            '7698':'Atlanta Reign',
            '4402': 'Boston Uprising',
            '7692': 'Chengdu Hunters',
            '4523': 'Dallas Fuel',
            '4407': 'Florida Mayhem',
            '7699': 'Guangzhou Charge',
            '7693': 'Hangzhou Spark',
            '4525': 'Houston Outlaws',
            '4410': 'London Spitfire',
            '4406': 'Los Angeles Gladiators',
            '4405': 'Los Angeles Valiant',
            '4403': 'New York Excelsior',
            '7694': 'Paris Eternal',
            '4524': 'Philadelphia Fusion',
            '4404': 'San Francisco Shock',
            '4409': 'Seoul Dynasty',
            '4408': 'Shanghai Dragons',
            '7695': 'Toronto Defiant',
            '7696': 'Vancouver Titans',
            '7697': 'Washington Justice'
        }

    def get_dict(self):
        return self.dict_data
