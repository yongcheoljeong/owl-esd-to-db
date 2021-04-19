scrimTableheaders = [
    'Map',
    'Section',
    'Point',
    'RoundName',
    'Timestamp',
    'Team',
    'Player',
    'Hero',
    'HeroDamageDealt', 
    'BarrierDamageDealt', 
    'DamageTaken', 
    'Deaths', 
    'Eliminations',
    'FinalBlows',
    'EnvironmentalDeaths',
    'EnvironmentalKills', 
    'HealingDealt', 
    'ObjectiveKills', 
    'SoloKills', 
    'UltimatesEarned', 
    'UltimatesUsed',
    'HealingReceived', 
    'UltimateCharge',
    'Cooldown1',
    'Cooldown2',
    'CooldownSecondaryFire',
    'CooldownCrouching',
    'IsAlive',
    'TimeElapsed',
    'Position',
    'MaxHealth',
    'DeathByHero',
    'DeathByAbility',
    'DeathByPlayer',
    'Resurrected',
    'DuplicatedHero',
    'DuplicateStatus',
    'Health',
    'DefensiveAssists',
    'OffensiveAssists',
]

header_transform_dict = {
    'map_name':'Map',
    'num_round':'Section',
    'Point':'Point',
    'round_name':'RoundName',
    'time':'Timestamp',
    'team_name':'Team',
    'player_name':'Player',
    'hero_name':'Hero',
}

ssg_dict = {
    'TimePlayed':33,
    'HeroDamageDealt':1207,
    'BarrierDamageDealt':1301,
    'HeroDamageTaken':401,
    'Deaths':42,
    'Eliminations':37,
    'FinalBlows':43,
    'EnvironmentalDeaths':869,
    'EnvironmentalKills':866,
    'HealingDealt':449,
    'ObjectiveKills':796,
    'SoloKills':45,
    'UltimatesEarned':1122,
    'UltimatesUsed':1123,
    'HealingReceived':1716
}

class MatchFinalStat:
    '''
    fromGameResult = [
        'esports_match_id',
        'start_time',
        'end_time',
        'num_map',
        'map_name',
        'map_type',
        'map_winner',
        'team_one_name',
        'team_two_name',
        'team_one_score',
        'team_two_score'
    ]
    fromGameInfo = [
        'esports_match_id',
        'time',
        'num_map',
        'map_name',
        'num_round',
        'round_name',
        'attacking_team_name',
        'team_one_payload_distance',
        'team_two_payload_distance',
        'team_one_time_banked',
        'team_two_time_banked',
        'context' # filter with 'END_ROUND'
    ]
    fromPlayerStatus = [
        'esports_match_id',
        'time',
        'num_map',
        'team_name',
        'player_name',
        'hero_name',
        'health',
        'ultimate_percent',
        'is_alive'
    ]
    '''