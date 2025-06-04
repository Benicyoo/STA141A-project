#endpoints we're using
from nba_api.stats.endpoints import leaguedashteamstats, leaguegamefinder
import pandas as pd
import time

SEASONS = [f"{y}-{str(y+1)[-2:]}" for y in range(2000, 2024)]
TYPE = ["Base", "Advanced", "Misc", "Four Factors"]
NBA_FINALISTS = {
    "2000-01": ["Los Angeles Lakers", "Philadelphia 76ers"],
    "2001-02": ["Los Angeles Lakers", "New Jersey Nets"],
    "2002-03": ["San Antonio Spurs", "New Jersey Nets"],
    "2003-04": ["Detroit Pistons", "Los Angeles Lakers"],
    "2004-05": ["San Antonio Spurs", "Detroit Pistons"],
    "2005-06": ["Miami Heat", "Dallas Mavericks"],
    "2006-07": ["San Antonio Spurs", "Cleveland Cavaliers"],
    "2007-08": ["Boston Celtics", "Los Angeles Lakers"],
    "2008-09": ["Los Angeles Lakers", "Orlando Magic"],
    "2009-10": ["Los Angeles Lakers", "Boston Celtics"],
    "2010-11": ["Dallas Mavericks", "Miami Heat"],
    "2011-12": ["Miami Heat", "Oklahoma City Thunder"],
    "2012-13": ["Miami Heat", "San Antonio Spurs"],
    "2013-14": ["San Antonio Spurs", "Miami Heat"],
    "2014-15": ["Golden State Warriors", "Cleveland Cavaliers"],
    "2015-16": ["Cleveland Cavaliers", "Golden State Warriors"],
    "2016-17": ["Golden State Warriors", "Cleveland Cavaliers"],
    "2017-18": ["Golden State Warriors", "Cleveland Cavaliers"],
    "2018-19": ["Toronto Raptors", "Golden State Warriors"],
    "2019-20": ["Los Angeles Lakers", "Miami Heat"],
    "2020-21": ["Milwaukee Bucks", "Phoenix Suns"],
    "2021-22": ["Golden State Warriors", "Boston Celtics"],
    "2022-23": ["Denver Nuggets", "Miami Heat"],
    "2023-24": ["Boston Celtics", "Dallas Mavericks"]
}
FINALS_WINNERS = {
    "2000-01": "Los Angeles Lakers",
    "2001-02": "Los Angeles Lakers",
    "2002-03": "San Antonio Spurs",
    "2003-04": "Detroit Pistons",
    "2004-05": "San Antonio Spurs",
    "2005-06": "Miami Heat",
    "2006-07": "San Antonio Spurs",
    "2007-08": "Boston Celtics",
    "2008-09": "Los Angeles Lakers",
    "2009-10": "Los Angeles Lakers",
    "2010-11": "Dallas Mavericks",
    "2011-12": "Miami Heat",
    "2012-13": "Miami Heat",
    "2013-14": "San Antonio Spurs",
    "2014-15": "Golden State Warriors",
    "2015-16": "Cleveland Cavaliers",
    "2016-17": "Golden State Warriors",
    "2017-18": "Golden State Warriors",
    "2018-19": "Toronto Raptors",
    "2019-20": "Los Angeles Lakers",
    "2020-21": "Milwaukee Bucks",
    "2021-22": "Golden State Warriors",
    "2022-23": "Denver Nuggets",
    "2023-24": "Boston Celtics"
}

#Acquire the Data
def get_team_stats(season, season_type, measure_type):
    stats = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        season_type_all_star=season_type,
        measure_type_detailed_defense=measure_type,
        per_mode_detailed='PerGame'
    )
    df = stats.get_data_frames()[0]
    return df

'''
for type in TYPE:
    # Regular season stats
    regular_dfs = []
    for season in SEASONS:
        df_regular = get_team_stats(season, 'Regular Season', type)
        df_regular["SEASON"] = season
        regular_dfs.append(df_regular)
        time.sleep(2)

    combined_regular_dfs = pd.concat(regular_dfs, ignore_index=True)
    reg_name = "regular_season_2000-2024_" + type +".csv"

    # Playoff stats
    playoff_dfs = []
    for season in SEASONS:
        df_playoffs = get_team_stats(season, 'Playoffs', type)
        df_playoffs["SEASON"] = season
        playoff_dfs.append(df_playoffs)
        time.sleep(2)
        
    combined_playoff_dfs = pd.concat(playoff_dfs, ignore_index=True)
    play_name = "playoff_season_2000-2024_" + type + ".csv"

    finalists_regular = combined_regular_dfs[
        combined_regular_dfs.apply(lambda row: row["TEAM_NAME"] in NBA_FINALISTS.get(row["SEASON"], []), axis=1)
    ]
    finalists_playoff = combined_playoff_dfs[
        combined_playoff_dfs.apply(lambda row: row["TEAM_NAME"] in NBA_FINALISTS.get(row["SEASON"], []), axis=1)
    ]
    # Save filtered data
    finalists_regular.to_csv(f"finalists_regular_season_2000-2024_{type}.csv", index=False)
    finalists_playoff.to_csv(f"finalists_playoff_season_2000-2024_{type}.csv", index=False)

    #doesnt make sense to combine data for some attributes as they are either ranks, proportions, ratings etc that dont make sense when combined !!!could filter them out when combining!!!
    #!!!!!if we do combine we should combine after filtering since all finalists would have played in the playoffs

    # Create combined datasets ### CAUTION
    reg_play_combined_df = pd.concat([combined_regular_dfs, combined_playoff_dfs], ignore_index=True)
    # Aggregate numeric columns by TEAM_ID and SEASON
    aggregated = reg_play_combined_df.groupby(['TEAM_ID', 'SEASON'], as_index=False).sum(numeric_only=True)
    # Get the first TEAM_NAME for each group
    team_names = reg_play_combined_df.groupby(['TEAM_ID', 'SEASON'])['TEAM_NAME'].first().reset_index()
    # Merge team names back
    aggregated = pd.merge(aggregated, team_names, on=['TEAM_ID', 'SEASON'])
    # Reorder columns
    cols = ['TEAM_ID', 'TEAM_NAME', 'SEASON'] + [col for col in aggregated.columns if col not in ['TEAM_ID', 'TEAM_NAME', 'SEASON']]
    aggregated = aggregated[cols]
    
    #CAUTION EXPLAINED: NOT EVERY TEAM MAKES IT TO THE PLAYOFFS ONLY SOME TEAMS GET A BOOST IN STATS FOR THE YEAR
    #other problem if stats are proportions cant just combine them willy nilly
    aggregated.to_csv("regular_playoff_2000-2024_" + type + ".csv", index=False)
'''
# Starting Preprocessing 

#Data finished being acquired
#1. Add finals win/loss column

#2. Filter 
#read in finalists playoff seasons and drop all columns except NET_RATING, PACE, TS_PCT, EFG_PCT, AST_RATIO, OREB_PCT, DREB_PCT, REB_PCT, TM_TOV_PCT, 


TYPE_COLS_TO_KEEP = {
    "Base": [
        "TEAM_ID", "TEAM_NAME", "W_PCT", "SEASON",
        "FG3M_RANK", "FG3A_RANK", "FG3_PCT", "PLUS_MINUS"
    ],
    "Advanced": [
        "TEAM_ID", "TEAM_NAME", "SEASON",
        "NET_RATING", "PACE", "TS_PCT", "EFG_PCT", "AST_RATIO",
        "OREB_PCT", "DREB_PCT", "REB_PCT", "TM_TOV_PCT"
    ],
    "Misc": [
        "TEAM_ID", "TEAM_NAME", "SEASON",
        "PTS_OFF_TOV", "PTS_2ND_CHANCE", "PTS_FB", "PTS_PAINT",
        "OPP_PTS_OFF_TOV", "OPP_PTS_2ND_CHANCE", "OPP_PTS_FB", "OPP_PTS_PAINT"
    ],
    "Four Factors": [
        "TEAM_ID", "TEAM_NAME", "SEASON",
        "FTA_RATE", "EFG_PCT", "OREB_PCT", "TM_TOV_PCT"
    ]
}

for type in TYPE:
    reg = pd.read_csv(f"./rawdatasets/finalists_regular_season_2000-2024_{type}.csv")
    filtered = reg[TYPE_COLS_TO_KEEP[type]]
    filtered.to_csv(f"./filtereddatasets/filtered_finalists_regular_{type}.csv", index=False)
for type in TYPE:
    play = pd.read_csv(f"./rawdatasets/finalists_playoff_season_2000-2024_{type}.csv")
    filtered = play[TYPE_COLS_TO_KEEP[type]]
    filtered.to_csv(f"./filtereddatasets/filtered_finalists_playoffs_{type}.csv", index=False)



reg_base = pd.read_csv("./filtereddatasets/filtered_finalists_regular_Base.csv")
reg_adv = pd.read_csv("./filtereddatasets/filtered_finalists_regular_Advanced.csv")
reg_misc = pd.read_csv("./filtereddatasets/filtered_finalists_regular_Misc.csv")
reg_four = pd.read_csv("./filtereddatasets/filtered_finalists_regular_Four Factors.csv")

# Merge on TEAM_ID, TEAM_NAME, SEASON
regular_merged = reg_base.merge(reg_adv, on=["TEAM_ID", "TEAM_NAME", "SEASON"], how="outer") \
                         .merge(reg_misc, on=["TEAM_ID", "TEAM_NAME", "SEASON"], how="outer") \
                         .merge(reg_four, on=["TEAM_ID", "TEAM_NAME", "SEASON"], how="outer")
# Sort and save merged regular season data
regular_merged = regular_merged.sort_values(by="SEASON")
regular_merged["WON_FINALS"] = regular_merged.apply(lambda row: 1 if FINALS_WINNERS.get(row["SEASON"]) == row["TEAM_NAME"] else 0, axis = 1)
regular_merged.to_csv("./filtereddatasets/merged_finalists_regular.csv", index=False)

# Repeat for playoffs

play_base = pd.read_csv("./filtereddatasets/filtered_finalists_playoffs_Base.csv")
play_adv = pd.read_csv("./filtereddatasets/filtered_finalists_playoffs_Advanced.csv")
play_misc = pd.read_csv("./filtereddatasets/filtered_finalists_playoffs_Misc.csv")
play_four = pd.read_csv("./filtereddatasets/filtered_finalists_playoffs_Four Factors.csv")

playoffs_merged = play_base.merge(play_adv, on=["TEAM_ID", "TEAM_NAME", "SEASON"], how="outer") \
                           .merge(play_misc, on=["TEAM_ID", "TEAM_NAME", "SEASON"], how="outer") \
                           .merge(play_four, on=["TEAM_ID", "TEAM_NAME", "SEASON"], how="outer")

# Sort and save merged playoffs data


playoffs_merged = playoffs_merged.sort_values(by="SEASON")
playoffs_merged["WON_FINALS"] = playoffs_merged.apply(lambda row: 1 if FINALS_WINNERS.get(row["SEASON"]) == row["TEAM_NAME"] else 0, axis = 1)
playoffs_merged.to_csv("./filtereddatasets/merged_finalists_playoffs.csv", index=False)
