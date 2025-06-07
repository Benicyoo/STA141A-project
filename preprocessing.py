import pandas as pd
import glob
import os

filter_cols = [
    "TEAM_ID", "TEAM_ABBREVIATION", "GAME_ID", "GAME_DATE", "MATCHUP", "WL", 
    "FGM", "FGA", "FG_PCT", 
    "FG3M", "FG3A", "FG3_PCT", 
    "FTM", "FTA", "FT_PCT", 
    "OREB", "DREB", "REB", 
    "AST", "STL", "BLK", "TOV",  
    "PTS", "PLUS_MINUS", "SEASON"
]

raw_folder = './raw'
filtered_folder = './filtered'
averaged_folder = './averaged'
os.makedirs(filtered_folder, exist_ok=True)
csv_files = glob.glob(os.path.join(raw_folder, '*.csv'))

for file in csv_files:
    df = pd.read_csv(file)
    # Filter columns
    df = df[filter_cols].copy()
    # Add Four Factors
    df['FTA_RATE'] = (df['FTA'] / df['FGA']).round(3)
    df['EFG_PCT'] = ((df['FGM'] + 0.5 * df['FG3M']) / df['FGA']).round(3)
    # For OREB_PCT, need opponent DREB for each game
    opp_dreb = df.groupby('GAME_ID')['DREB'].transform(lambda x: x[::-1].values)
    df['OREB_PCT'] = (df['OREB'] / (df['OREB'] + opp_dreb)).round(3)
    df['TOV_RATE'] = (df['TOV'] / (df['FGA'] + 0.44 * df['FTA'] + df['TOV'])).round(3)
    # Save processed file to filtered folder
    out_path = os.path.join(filtered_folder, f"processed_{os.path.basename(file)}")
    df.to_csv(out_path, index=False)

# Compute rolling averages for each team within each season
# For each game, calculate the average of all previous games in the season for that team
rolling_features = [
    'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT',
    'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PTS', 'PLUS_MINUS',
    'FTA_RATE', 'EFG_PCT', 'OREB_PCT', 'TOV_RATE'
]

all_rolling_regular = []
all_rolling_playoffs = []

for file in glob.glob(os.path.join(filtered_folder, 'processed_*.csv')):
    df = pd.read_csv(file)
    df = df.sort_values(['TEAM_ID', 'SEASON', 'GAME_DATE'])
    # Calculate expanding mean (excluding current game) for each team in each season
    rolling_data = {}
    rolling_data['TEAM_ID'] = df['TEAM_ID']
    rolling_data['GAME_ID'] = df['GAME_ID']
    rolling_data['GAME_DATE'] = df['GAME_DATE']
    rolling_data['SEASON'] = df['SEASON']
    rolling_data['WL'] = df['WL']
    for feat in rolling_features:
        rolling_data[f'AVG_{feat}'] = (
            df.groupby(['TEAM_ID', 'SEASON'])[feat]
            .expanding()
            .mean()
            .round(3)
            .reset_index(level=[0,1], drop=True)
        )
    df_rolling = pd.DataFrame(rolling_data)
    filename = os.path.basename(file).lower()
    if "playoff" in filename:
        all_rolling_playoffs.append(df_rolling)
    else:
        all_rolling_regular.append(df_rolling)

# Concatenate and save
if all_rolling_regular:
    rolling_df_regular = pd.concat(all_rolling_regular, ignore_index=True)
    rolling_df_regular.to_csv(os.path.join(averaged_folder, 'rolling_averages_regular_season.csv'), index=False)

if all_rolling_playoffs:
    rolling_df_playoffs = pd.concat(all_rolling_playoffs, ignore_index=True)
    rolling_df_playoffs.to_csv(os.path.join(averaged_folder, 'rolling_averages_playoffs.csv'), index=False)