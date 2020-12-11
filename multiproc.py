from getgameinfo import to_csv
from getgameinfo import esports_match_ids
import pandas as pd
import json
import numpy as np


def multiproc(esports_match_id):

    match_df = to_csv[to_csv['esports_match_id'] == esports_match_id]
    match_df = match_df.reset_index(drop = True)

    df_info = pd.DataFrame()
    df_score = pd.DataFrame()
    df_team0 = pd.DataFrame()
    df_team1 = pd.DataFrame()
    team_list = ['Home_', 'Away_']

    for i in match_df.index:
        info_json = json.loads(match_df.loc[i, 'info'])
        to_df_info_json = pd.json_normalize(info_json)
        to_df_info_json.index = [i]
        df_info = df_info.append(to_df_info_json, ignore_index = True)

        score_json = json.loads(match_df.loc[i,'score_info'])
        to_df_score_json = pd.json_normalize(score_json)
        to_df_score_json.index = [i]
        df_score = df_score.append(to_df_score_json, ignore_index = True)
        
        to_df_team0_json = pd.json_normalize(score_json['team_info'][0])
        to_df_team0_json = to_df_team0_json.add_prefix(team_list[0])
        to_df_team0_json.index = [i]
        df_team0 = df_team0.append(to_df_team0_json, ignore_index = True)

        to_df_team1_json = pd.json_normalize(score_json['team_info'][1])
        to_df_team1_json = to_df_team1_json.add_prefix(team_list[1])
        to_df_team1_json.index = [i]
        df_team1 = df_team1.append(to_df_team1_json, ignore_index = True)

    match_df = pd.concat([match_df, df_info, df_score, df_team0, df_team1], axis = 1)
    print(f'{esports_match_id} has been successfully saved')
    match_df.to_pickle(f'pickles/{esports_match_id}.pickle')