#endpoints we're using
from nba_api.stats.endpoints import leaguegamelog
import pandas as pd
import time

SEASONS = [f"{y}-{str(y+1)[-2:]}" for y in range(1996, 2024)]

for type in ['Regular Season', 'Playoffs']:
    all_games=[]
    for season in SEASONS:
        gamelog = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=type)
        df = gamelog.get_data_frames()[0]
        df['SEASON'] = season
        all_games.append(df)
        time.sleep(2)  # avoid rate limiting
        print(f"Finished season {season} for {type}")
    games_df = pd.concat(all_games, ignore_index=True)
    games_df.to_csv(f'./raw/{type}_games_1996_2024.csv', index=False)