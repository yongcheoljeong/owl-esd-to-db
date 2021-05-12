# OWL Event Stream Data Parser

![NYXLDataTeam](https://images.blz-contentstack.com/v3/assets/bltcade263868472d53/bltc0a07efe6ff22e51/5d63584ecd4bff10737c98f1/Team_Logos_NYXL.svg?auto=webp)

NYXL Data Team

## How to use
1. Download Match Event Stream Data from DAM (https://dam.btg.blizzard.com/)
2. Unzip the file (the file is weekly-based ESD)
3. Replace the file's path to root_dir main.ipynb
4. Default `if_exists = 'append'` to append ESD which is cut due to time zone. If you replace them in DB, set `if_exists = 'replace'`

```python
root_dir = r'D:\2021_EventStreamData\20210510'
if_exists = 'append'
```

---
## Documentations

## OWL Event Stream Data
### class
- GameInfo
    + 'esports_match_id':esports_match_id,
    + 'time':time,
    + 'num_map':num_map,
    + 'map_name':map_guid,
    + 'map_type':map_type,
    + 'num_round':num_round,
    + 'round_name':round_name,
    + 'team_one_name':team_one_esports_team_id,
    + 'team_two_name':team_two_esports_team_id,
    + 'attacking_team_name':attacking_team_id,
    + 'team_one_score':team_one_score,
    + 'team_two_score':team_two_score,
    + 'team_one_payload_distance':team_one_payload_distance,
    + 'team_two_payload_distance':team_two_payload_distance,
    + 'team_one_time_banked':team_one_time_banked,
    + 'team_two_time_banked':team_two_time_banked,
    + 'context':context
- GameStart
    + 'esports_match_id':esports_match_id,
    + 'start_time':time,
    + 'num_map':num_map,
    + 'map_name':map_guid,
    + 'map_type':map_type
- GameResult
    + 'esports_match_id':esports_match_id,
    + 'end_time':time,
    + 'total_game_time':(int(total_game_time_ms) / 1000),
    + 'num_map':num_map,
    + 'map_name':map_guid,
    + 'map_type':map_type,
    + 'map_winner':winner_esports_team_id,
    + 'team_one_name':team_one_esports_team_id,
    + 'team_two_name':team_two_esports_team_id,
    + 'team_one_score':team_one_score,
    + 'team_two_score':team_two_score
- Kill
    + 'esports_match_id':esports_match_id,
    + 'time':time,
    + 'num_map':num_map,
    + 'map_name':map_name,
    + 'map_type':map_type,
    + 'killed_player_id':killed_player_id,
    + 'killed_player_hero_name':killed_player_hero_name,
    + 'final_blow_player_id':final_blow_player_id,
    + 'death_position':death_position,
    + 'killer_position':killer_position,
    + 'killed_pet':killed_pet
- PlayerStatus
    + 'esports_match_id':esports_match_id,
    + 'time':time,
    + 'num_map':num_map,
    + 'map_name':map_name,
    + 'map_type':map_type,
    + 'team_name':team_name,
    + 'player_name':player_name,
    + 'hero_name':hero_name,
    + 'health':health,
    + 'ultimate_percent':ultimate_percent,
    + 'is_ultimate_ready':is_ultimate_ready,
    + 'is_alive':is_alive,
    + 'position':position
- PlayerHeroStats
    + time,
    + hero_name,
    + stat_lifespan, 
    + ssg, 
    + amount, 
    + stat_name, 
    + player_name, 
    + team_name

## GUID
### class
- GUIDMap
- GUIDHero
- GUIDStat
    + ShortStatGuid provided by OWL
- GUIDTeam